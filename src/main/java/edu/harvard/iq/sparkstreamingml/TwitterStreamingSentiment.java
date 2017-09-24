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
 * TODO: remove duplicate feature creation code
 * TODO: PUT model path in argument list or separate conf file
 * QUESTION:  do we need to broadcast model?  
 * @author ellenk
 */
public class TwitterStreamingSentiment {

    static final String MODEL_PATH = "/Users/ellenk/src/SparkStreamingML/data/naiveBayes";

    public static void main(String args[]) {
        
        setTwitterProps(args);       
        analyzeTweets();
    }

    private static void analyzeTweets() {
         
        SparkConf sparkConf = new SparkConf().setAppName(" Twitter Sentiment Demo ");
        
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(5000L));
        JavaDStream<Status> dStream = TwitterUtils.createStream(streamingContext);
        NaiveBayesModel model = ModelSingleton.getInstance(sparkConf);
    
        String[] filterWords = { "happy","sad", "love", "hate", "good", "bad" };
        
        JavaDStream<String> englishTweets = dStream
                .filter(status -> status.getLang().equals("en"))
                .map(status -> status.getText())
                .filter(text -> Arrays.stream(filterWords).anyMatch(text::contains) );
              
        englishTweets.foreachRDD(rdd -> {
                SparkSession sparkSingleton = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                JavaRDD<TweetRecord> tweetRDD = rdd.map((String s) -> {return new TweetRecord(s);});
                Dataset<Row> statusDF = sparkSingleton.createDataFrame(tweetRDD, TweetRecord.class);
                //  statusDF.show();
                Dataset<Row> featuresDF = getFeatures(statusDF);
                // featuresDF.show();
                Dataset<Row> predictionsDF = model.transform(featuresDF);
               // predictionsDF.show();   
                
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



    
    private static Dataset<Row> getFeatures(Dataset<Row> statusDF) {
       
        // Split the "status" column in to words
        Dataset<Row> wordsData = new Tokenizer().setInputCol("status").setOutputCol("words").transform(statusDF);

        // Remove stop words from the raw list of words  
        Dataset<Row> filtered = new StopWordsRemover().setInputCol("words").setOutputCol("filtered").transform(wordsData);

        // Use Hashing algorithm to transform filtered words into feature vector
        Dataset<Row> features = new HashingTF().setNumFeatures(2000).setInputCol("filtered").setOutputCol("features").transform(filtered);
    
        return features;
    }
    
    private static void setTwitterProps(String args[]) { 
     if (args.length < 4) {
            System.err.println("Usage: TwitterStreamingSentiment <consumer key>"
                    + " <consumer secret> <access token> <access token secret> [<filters>]");
            System.exit(1);
        }

        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", args[0]);
        System.setProperty("twitter4j.oauth.consumerSecret", args[1]);
        System.setProperty("twitter4j.oauth.accessToken", args[2]);
        System.setProperty("twitter4j.oauth.accessTokenSecret", args[3]);
    }
    
}

class ModelSingleton {
    private static transient NaiveBayesModel instance = null;
    public static NaiveBayesModel getInstance(SparkConf sparkConf) {
        if (instance == null) {
            JavaSparkSessionSingleton.getInstance(sparkConf);
            instance = NaiveBayesModel.load(TwitterStreamingSentiment.MODEL_PATH);
        }
     
        return instance;
    }
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
  private static transient SparkSession instance = null;
  public static SparkSession getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();
    }
    return instance;
  }
  
  
}
