
package edu.harvard.iq.sparkstreamingml;


import java.io.IOException;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Create model for Sentiment Analysis using a training dataset of 1.6 million
 * tweets.
 * Source of training data-
 * http://help.sentiment140.com/for-students
 * Paper describing how training data was gathered and analyzed:
 * http://cs.stanford.edu/people/alecmgo/papers/TwitterDistantSupervision09.pdf
 * @author ellenk
 */


public class CreateNaiveBayesModel {

    public static void main(String args[]) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: CreateNaiveBayesModel <training path> <model path>");                  
            System.exit(1);
        }
        String trainingPath = args[0];
        String modelPath = args[1];
        SparkSession session = SparkSession
                .builder()
                .appName("Create Pipeline Model")
                .getOrCreate();

        String sentiment140Path = trainingPath;

        Dataset<Row> training = loadSentiment140File(session, sentiment140Path);
        Dataset<Row> status = training.select("label", "status");
        
        Tokenizer tokenizer = new Tokenizer().setInputCol("status").setOutputCol("words");
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol()).setOutputCol("filtered");
        HashingTF hashingTF = new HashingTF().setNumFeatures(2000).setInputCol(stopWordsRemover.getOutputCol()).setOutputCol("features");
        NaiveBayes naiveBayes = new NaiveBayes();
        
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, stopWordsRemover, hashingTF, naiveBayes});
      
        PipelineModel model = pipeline.fit(status);
       
        model.write().overwrite().save(modelPath);
       

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
