/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;
    
/**
 * Reads the Kafka Queue generated by HashTags.java, and
 * aggregates the hash tag counts by time window using structured streaming.
 * @author ellenk
 */
public class TwitterTrends {

    public static void main(String args[]) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Twitter Trend Example")
                .master("local[4]")
                .getOrCreate();
      
    
        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "trendsDemo")
                .option("startingOffsets", "earliest") 
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
               
        Dataset<EnhancedTweetRecord> records = lines.map((String s) -> {return new EnhancedTweetRecord(s);}, Encoders.bean(EnhancedTweetRecord.class));
        
        // flatMap() to HashTags with corresponding timestamp of Tweet
        Dataset<Row> hashTags = records.flatMap(record -> { 
            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
          for (String word : record.getStatus().split(" ")) {
            if (word.startsWith("#")) {  
                result.add(new Tuple2<>(word,record.getCreatedAt()));
            }
          }
          return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
      ).toDF("word", "timestamp");
        
        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = hashTags
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                        functions.window(hashTags.col("timestamp"), "10 minutes", "5 minutes"),
                        hashTags.col("word")
                ).count();
        
        Dataset<Row> filtered = windowedCounts.filter(col("count").gt(1));

        // Generate running word count
       // Dataset<Row> wordCounts = lines.flatMap(
       //          
       //         (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(",")).iterator(),
       //         Encoders.STRING()).groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = filtered.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        /*
        Every streaming source is assumed to have offsets
        (similar to Kafka offsets, or Kinesis sequence numbers) to track the read position in the stream. 
        The engine uses checkpointing and write ahead logs to record the offset range of the data being processed in each trigger
        */
      //  StreamingQuery fileQuery
      //  = lines.writeStream()
      //          .format("parquet") // can be "orc", "json", "csv", etc.
      //          .option("path", "/tmp/demo")
      //          .option("checkpointLocation", "/tmp/democheckpoint")
      //          .outputMode("complete")
      //          .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            System.out.println(e);
        }
    }
}
