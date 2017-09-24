
package edu.harvard.iq.sparkstreamingml;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author ellenk
 */
/*

/Applications/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.sparkstreamingml.CreateNaiveBayesModel --master spark://Ellens-MacBook-Pro-2.local:7077   --verbose  /Users/ellenk/src/SparkStreamingML/target/SparkStreamingML-1.0-SNAPSHOT.jar 

*/
public class CreateNaiveBayesModel {

    public static void main(String args[]) {
       
            SparkSession session = SparkSession
                .builder()
                .appName("Create NaiveBayes Model")
                 .master("local[2]")
                .getOrCreate();
            
        String sentiment140Path = "/Users/ellenk/Downloads/trainingandtestdata_2/training.1600000.processed.noemoticon.csv";
        
        Dataset<Row> training = loadSentiment140File(session, sentiment140Path);
        Dataset<Row> selected = training.select("label","status");
        
        
        // Split the "status" column in to words
        Tokenizer tokenizer = new Tokenizer().setInputCol("status").setOutputCol("words");
        Dataset<Row> words = tokenizer.transform(selected);
        words.show();
         // Remove stop words from the raw list of words
        StopWordsRemover remover = new StopWordsRemover()
        .setInputCol("words")
        .setOutputCol("filtered");       
        Dataset<Row> filteredData = remover.transform(words);
        
           
        // Create a feature vector from the words
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features")
                .setVocabSize(20000)
                .setMinDF(10)
                .fit(filteredData);
        Dataset<Row> rawfeatures = cvModel.transform(filteredData);
        rawfeatures.show();

     NaiveBayes naiveBayes = new NaiveBayes();
     NaiveBayesModel model= naiveBayes.fit(rawfeatures);
     try {
        model.write().overwrite().save("/Users/ellenk/src/SparkStreamingML/data/naiveBayes");
     } catch(IOException e) {
         System.out.println("Exception saving model: " + e.getMessage());
     }
        
      

      session.stop();
    }
    
    /**
    * Loads the Sentiment140 file from the specified path using SparkContext.
    *
    * @param session                   -- Spark Session.
    * @param sentiment140FilePath -- Absolute file path of Sentiment140.
    * @return -- Spark DataFrame of the Sentiment file with the tweet text and its polarity.
    */
  public static Dataset<Row>  loadSentiment140File(SparkSession session, String sentiment140FilePath) {
    
    Dataset<Row> tweetsDF = session.read()
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath)
      .toDF("label", "id", "date", "query", "user", "status");

    // Drop the columns we are not interested in.
    return tweetsDF.drop("id").drop("date").drop("query").drop("user");
  }
}
