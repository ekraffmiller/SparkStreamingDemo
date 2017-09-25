/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
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
public class HashTagSentiment {
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
        
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(5000L));
        JavaDStream<Status> dStream = TwitterUtils.createStream(streamingContext);
        PipelineModel model = ModelSingleton.getInstance(sparkConf,modelPath);
    
        // From the live stream, filter English language tweets
        // that contain hashtag
        JavaDStream<String> englishTweets = dStream
                .filter(status -> status.getLang().equals("en"))
                .map(status -> status.getText())
                .filter(text -> text.contains("#") );
              
        englishTweets.foreachRDD(rdd -> {
                // we need handle to SparkSession to get Dataframe
                SparkSession sparkSingleton = SparkSessionSingleton.getInstance(rdd.context().getConf());          

                // Convert the RDD to a Dataframe, so we can use the model
                JavaRDD<TweetRecord> tweetRDD = rdd.map((String s) -> {return new TweetRecord(s);});
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
                        prod.send(new ProducerRecord("twitterDemo",row.getAs("prediction") + " -- " + row.getAs("status")));
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


  
  

