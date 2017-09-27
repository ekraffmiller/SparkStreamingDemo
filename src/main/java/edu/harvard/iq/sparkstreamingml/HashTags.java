/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 *  * QUESTION:  do we need to broadcast model?  
 * @author ellenk
 */
public class HashTags {
 private static String modelPath;
    
    public static void main(String args[]) {
         if (args.length < 5) {
            System.err.println("Usage: HashTagSentiment <consumer key>"
                    + " <consumer secret> <access token> <access token secret> <model path>");
            System.exit(1);
        }
        setTwitterAuth(args);  
        modelPath = args[4];

        analyzeHashtagTweets();
    }

    private static void analyzeHashtagTweets() {
         
        SparkConf sparkConf = new SparkConf().setAppName(" Twitter Hashtag Demo ");
        SparkSession sparkSession = SparkSessionSingleton.getInstance(sparkConf);          

     
        JavaStreamingContext streamingContext = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()), new Duration(5000L));
        JavaDStream<Status> dStream = TwitterUtils.createStream(streamingContext);
        PipelineModel model = PipelineModel.load(modelPath);
     
        // From the live stream, filter English language tweets
        // that contain hashtags
        JavaDStream<Status> englishTweets = dStream
                .filter(status -> status.getLang().equals("en") && status.getText().contains("#"));

              
        englishTweets.foreachRDD(rdd -> {
                // we need handle to SparkSession to get Dataframe
                SparkSession sparkSingleton = SparkSessionSingleton.getInstance(rdd.context().getConf());          

                // Convert the RDD to a Dataframe, so we can use the model
                JavaRDD<TweetRecord> tweetRDD = rdd.map((Status s) -> {return new TweetRecord(s.getText(),new Timestamp(s.getCreatedAt().getTime()));});
                Dataset<Row> statusDF = sparkSingleton.createDataFrame(tweetRDD, TweetRecord.class);
                
                // Model transformation adds "predictions" column to our DF
                Dataset<Row> predictionsDF = model.transform(statusDF);
                       
                
                // Send results to Kafka
                predictionsDF.foreachPartition(partition -> {                    
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "localhost:9092");
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
                    KafkaProducer prod = new KafkaProducer(props);
                    partition.forEachRemaining(row -> {
                              StringWriter sw = new StringWriter();
                              CSVWriter writer = new CSVWriter(sw);
                              writer.writeNext(new String[]{row.getAs("prediction").toString(),row.getAs("status"),row.getAs("createdAt").toString()});
                              prod.send(new ProducerRecord("trendsDemo3",sw.getBuffer().toString()));
                    });
                    prod.close();
                });
             
            });

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("Stream Interrupted");
        }

    }



    
    private static void setTwitterAuth(String args[]) { 
    

        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", args[0]);
        System.setProperty("twitter4j.oauth.consumerSecret", args[1]);
        System.setProperty("twitter4j.oauth.accessToken", args[2]);
        System.setProperty("twitter4j.oauth.accessTokenSecret", args[3]);
    }
 
    
}


  
  

