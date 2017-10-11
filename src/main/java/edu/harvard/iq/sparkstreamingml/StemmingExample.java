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
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.feature.Stemmer;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;


/**
 *  Test ability to do stemming, stop words and feature vectors within Spark
 * @author ellenk
 */
public class StemmingExample {

    public static void main(String args[]) throws IOException {
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        if (args.length < 1) {
            System.err.println("Usage: StemmingExample <csv path> ");
            System.exit(1);
        }
        String csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("Stemming Example")
                .getOrCreate();

        Dataset<Row> docText = loadCSVText(session, csvPath);
        Tuple2<Dataset<Row>,Dataset<Row>> result = analyzeText(docText);
        System.out.println("features dataframe: ");
        result._1.show(); 
        System.out.println("ngram dataframe: ");
        result._2.show();
 
        
        session.stop();
    }
    /**
     * Do stemming, stopword filtering, and create feature vector for text column
     * @param docTextDF - a Dataframe that must have two columns: 
     *      text - to be analyzed
     *      docIndex - to group by after flattening the text to individual words
     * @return - a Dataframe that contains the following columns:
     *      docIndex - id of doc within docset
     *      features - sparse array feature vector
     */
    public static Tuple2<Dataset<Row>,String[]> getDocFeatures(Dataset<Row> docStems) {
            // Group by docIndex to get all the stems for each Abstract back into a list
        Dataset<Row> grouped = docStems.groupBy(col("docIndex") ).agg(collect_list(col("stem")).alias("stemArray")).sort(col("docIndex"));
           
        // fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
        .setInputCol("stemArray")
        .setOutputCol("features")
        .setMinDF(2)                
        .fit(grouped);
     
        Dataset<Row> features = cvModel.transform(grouped).select("docIndex","features");
        return new Tuple2(features, cvModel.vocabulary());
    }
    
    public static Dataset<Row> getDocStems(Dataset<Row> docText) {
          // Split up Abstract text into array of words
        // Use RegexTokenizer to ignore punctuation
        RegexTokenizer tokenizer = new RegexTokenizer().setPattern("\\W").setInputCol("text").setOutputCol("words");
        Dataset<Row> tokenized = tokenizer.transform(docText);
        
        // Remove stopWords from array of words (stopWordsRemover will only work on an array, not single string word)
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("words").setOutputCol("filtered");        
        Dataset<Row> filtered = stopWordsRemover.transform(tokenized);
        
        // Flatten the filtered words into a row for each word. 
        // Need the docIndex column to reduce back to array of words
        Dataset<Row> flattened = filtered.flatMap(row -> {
            List<Tuple2<String,Integer>> result = new ArrayList<>();
            WrappedArray<String> list = (WrappedArray<String>)row.getAs("filtered");
            scala.collection.Iterator iter = list.iterator();
            while(iter.hasNext()) {
                result.add(new Tuple2((String)iter.next(), row.getAs("docIndex")));
            }           
            return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(),Encoders.INT())
        ).toDF("word","docIndex");

        // Get the stem for each word (Stemmer will only work on a single word, not on an array of words 
        Stemmer stemmer = new Stemmer().setLanguage("english").setInputCol("word").setOutputCol("stem");
        Dataset<Row> stemmed = stemmer.transform(flattened);
        
        return stemmed;
        
    }
    public static Dataset<Row> getNGrams(Dataset<Row> docStems, String[] vocabulary) {
           
        // Group words by stem in order to create the list of unstemmed words 
        // for each stem
        Dataset<Row> ngram = docStems.groupBy(col("stem")).agg(collect_set(col("word")).alias("unstemmed"));
        ngram.show();
        List vocabList = Arrays.asList(vocabulary);
        // Add index column from vocab[], and word = first unstemmed word in list
        // Now we have everything needed for NGram entity!
        StructType ngramSchema = ngram.schema();
        ngramSchema = ngramSchema.add("index", DataTypes.LongType);
        ngramSchema = ngramSchema.add("word", DataTypes.StringType);
        Dataset<Row> indexed = ngram.map(row -> {
            long index = vocabList.indexOf(row.getAs("stem"));         
            return RowFactory.create( row.getAs("stem"), row.getAs("unstemmed"),index,((WrappedArray<String>)row.getAs("unstemmed")).head());
        }, RowEncoder.apply(ngramSchema)).filter(col("index").gt(-1)).sort(col("index")).toDF();
        
        indexed.show(false);
        return indexed; 
        
    }
    
    public static Tuple2<Dataset<Row>,Dataset<Row>> analyzeText(Dataset<Row> docText) {
       Dataset<Row> docStems = getDocStems(docText);
       Tuple2<Dataset<Row>,String[]> features = getDocFeatures(docStems);
       Dataset<Row> ngrams = getNGrams(docStems, features._2);
       return new Tuple2(features._1,ngrams);
    }
    
    
    /**
     * Loads the Sentiment140 file from the specified path using SparkContext.
     *
     * @param session -- Spark Session.
     * @param csvPath -- Absolute file path of to csv file. (we are assuming
     * it's text column is called "text")
     * @return -- Spark DataFrame of the csv file with text and docIndex columns
     */
    public static Dataset<Row> loadCSVText(SparkSession session, String csvPath) {

        Dataset<Row> docTextDF = session.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvPath)
                .toDF();
       
        // some abracts are null, so dont use them.
        return docTextDF
                .filter(row -> row.getAs("text") != null);
    }
}
