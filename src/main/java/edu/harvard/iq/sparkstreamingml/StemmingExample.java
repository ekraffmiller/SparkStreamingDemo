/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.feature.Stemmer;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;


/**
 *  Test ability to do stemming, stop words and feature vectors within Spark
 * @author ellenk
 */
public class StemmingExample {

    public static void main(String args[]) throws IOException {
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        if (args.length < 1) {
            System.err.println("Usage: StemmingExample <csv path> ");
            System.exit(1);
        }
        String csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("Stemming Example")
                .getOrCreate();

        Dataset<Row> training = loadCSVText(session, csvPath);
        training.show();
        // Split up Abstract text into array of words
        Tokenizer tokenizer = new Tokenizer().setInputCol("Abstract Note").setOutputCol("words");
        Dataset<Row> tokenized = tokenizer.transform(training);
        
        // Remove stopWords from array of words (stopWordsRemover will only work on an array, not single string word)
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("words").setOutputCol("filtered");        
        Dataset<Row> filtered = stopWordsRemover.transform(tokenized);
        
        // Flatten the filtered words into a row for each word. 
        // Need the title column to reduce back to array of words
        Dataset<Row> flattened = filtered.flatMap(row -> {
            List<Tuple2<String,String>> result = new ArrayList<>();
            WrappedArray<String> list = (WrappedArray<String>)row.getAs("filtered");
            scala.collection.Iterator iter = list.iterator();
            while(iter.hasNext()) {
                result.add(new Tuple2((String)iter.next(), row.getAs("Title")));
            }           
            return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(),Encoders.STRING())
        ).toDF("word","title");

        // Get the stem for each word (Stemmer will only work on a single word, not on an array of words 
        Stemmer stemmer = new Stemmer().setLanguage("english").setInputCol("word").setOutputCol("stem");
        Dataset<Row> stemmed = stemmer.transform(flattened);
        
        // Group by title to get all the stems for each Abstract back into a list
        Dataset<Row> grouped = stemmed.groupBy(col("title") ).agg(collect_list(col("stem")).alias("stemArray")).sort(col("title"));
        
        // fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
        .setInputCol("stemArray")
        .setOutputCol("feature")
        .setVocabSize(1000)
        .setMinDF(2)
        .fit(grouped);
     
        Dataset<Row> features = cvModel.transform(grouped);
        features.show();
        String[] vocabulary = cvModel.vocabulary();
        List vocab = Arrays.asList(vocabulary);
        System.out.println(Arrays.toString(vocabulary));
        
        // Next things to do - 
        // > group words by stem
        // > use vocabulary array and grouped words by stem to create NGram
        // > save NGram in db!
        // > improve tokenizer to ignore punctuation
        Dataset<Row> ngram = stemmed.groupBy(col("stem")).agg(collect_set(col("word")).alias("unstemmed"));
        ngram.show();
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("index", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("stem", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("unstemmed", ArrayType.apply(DataTypes.StringType),true));
        
        ExpressionEncoder<Row> encoder = RowEncoder.apply(DataTypes.createStructType(fields));
        
        Dataset<Row> indexed = ngram.map(row -> {
            long index = vocab.indexOf(row.getAs("stem"));
            return RowFactory.create(index, row.getAs("stem"), row.getAs("unstemmed"));
        }, encoder).filter(col("index").gt(-1)).sort(col("index")).toDF();
        
        indexed.show(false);
     
        
        
        session.stop();
    }

    /**
     * Loads the Sentiment140 file from the specified path using SparkContext.
     *
     * @param session -- Spark Session.
     * @param csvPath -- Absolute file path of to csv file. (we are assuming
     * it's text column is called "abstract note")
     * @return -- Spark DataFrame of the Sentiment file with the tweet text and
     * its polarity.
     */
    public static Dataset<Row> loadCSVText(SparkSession session, String csvPath) {

        Dataset<Row> tweetsDF = session.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvPath)
                .toDF();

        // some abracts are null, so dont use them.
        return tweetsDF.filter(row -> row.getAs("Abstract Note") != null);
    }
}
