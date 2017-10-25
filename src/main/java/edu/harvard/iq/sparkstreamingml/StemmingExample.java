/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.sql.helpers.StructFields;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.feature.Stemmer;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.WrappedArray;


/**
 *  Test ability to do stemming, stop words and feature vectors within Spark
 * @author ellenk
 */
public class StemmingExample {
    private static String csvPath;
    public static void main(String args[]) throws IOException {
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        if (args.length < 1) {
            System.err.println("Usage: StemmingExample <csv path> ");
            System.exit(1);
        }
         csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("Stemming Example")
                .getOrCreate();
        String language = "english";
        boolean bigrams = false;
        int minDocFrequency = 2;
        Dataset<Row> docText = loadCSVText(session, csvPath);
        Tuple2<Dataset<Row>,Dataset<Row>> result = analyzeText(docText, "dummyClassifierId", language,bigrams, minDocFrequency);
        System.out.println("features dataframe: ");
        result._1.show(); 
        System.out.println("ngram dataframe: ");
        result._2.show(200);
        
      //  System.out.println("bigrams only:");
      //  result._2.filtesr(row -> row.getAs("stem").toString().contains(" ")).show();
 
        
        session.stop();
    }
    /**
     * create feature vectors for stemmed doc text
     * @param docStems - a Dataframe that must have two columns: 
     *      
     *      docIndex - to group by after flattening the text to individual stems
     *      stem - the word stem
     * @param minDocFrequency - the minimum number of documents the ngram
     *      has to appear in to be included in the vocabulary
     * @param classifierId  - for the WordDocCount collections (cId field)
     * @return - Tuple2
     *      1:  Dataset<Row> that contains the following columns:
     *      docIndex - id of doc within docset
     *      indexes - indexes of sparse feature vector
     *      counts - values of sparse feature vector
     *      2: String[] - model vocabulary - needed to create NGrams
     */
    public static Tuple2<Dataset<Row>, String[]> getDocFeatures(Dataset<Row> docStems, int minDocFrequency, String classifierId) {
        // Group by docIndex to get all the stems for each Abstract back into a list
        Dataset<Row> grouped = docStems.groupBy(col("docIndex")).agg(collect_list(col("stem")).alias("stemArray"));
        grouped.sort(col("docIndex")).show(200);
        // fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("stemArray")
                .setOutputCol("features")
                .setMinDF(minDocFrequency)
                .fit(grouped);

        Dataset<Row> features = cvModel.transform(grouped).select("docIndex", "features");

        List<StructField> fSchema = new ArrayList<>();
        fSchema.add(DataTypes.createStructField("d", DataTypes.IntegerType, false));
        fSchema.add(DataTypes.createStructField("indexes", DataTypes.createArrayType(DataTypes.IntegerType), false));
        fSchema.add(DataTypes.createStructField("counts", DataTypes.createArrayType(DataTypes.IntegerType), false));
        fSchema.add(DataTypes.createStructField("cId", DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fSchema.toArray(new StructField[fSchema.size()]));
        
        Dataset<Row> mappedFeatures = features.map(row -> {
            double[] values = ((SparseVector) row.getAs("features")).values();
            int[] intValues = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                intValues[i] = (int) Math.round(values[i]);
            }
            return RowFactory.create(
                    new Integer(row.getAs("docIndex")),
                    ((SparseVector) row.getAs("features")).indices(),
                    intValues,
                    classifierId
            );
        }, RowEncoder.apply(schema));
        mappedFeatures.show();

        mappedFeatures.printSchema();
        mappedFeatures.show(false);
        return new Tuple2(mappedFeatures, cvModel.vocabulary());
    }
    /**
     * From a Dataset of plain text, return a tranformed set containing word, stem and docIndex
     * @param docText - dataset with two columns
     *      text - all the text in the document
     *      docIndex - an id for this document within the Dataset (0 - (docsetSize-1))
     * @param stopWords - words to filter our before stemming
     * @param language - for stemming (different languages have different rules for stemming)
     * @param bigrams - if true, include bigrams in the feature vector
     * @return Dataset of stems. Columns:
     *      word - original word (or two words if bigram)
     *      stem of the word (or just a copy of the bigram)
     *      docIndex - identifies the document that the word belongs to
     */
    public static Dataset<Row> getDocStems(Dataset<Row> docText,String[] stopWords, String language, boolean bigrams) {
          // Split up Abstract text into array of words
        // Use RegexTokenizer to ignore punctuation
        RegexTokenizer tokenizer = new RegexTokenizer().setPattern("\\W").setInputCol("text").setOutputCol("words");
        Dataset<Row> tokenized = tokenizer.transform(docText);
        
        // Remove stopWords from array of words (stopWordsRemover will only work on an array, not single string word)
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setStopWords(stopWords).setInputCol("words").setOutputCol("filtered");        
        Dataset<Row> filtered = stopWordsRemover.transform(tokenized);
        Dataset<Row> ngrams = null;
        if (bigrams) {
            // for bigrams  we are not going to stem, just 
            // use the phrase as is (assuming this is the best way to find
            // important bigrams)
            NGram ngramTransformer = new NGram().setN(2).setInputCol("filtered").setOutputCol("ngrams");
            ngrams = ngramTransformer.transform(filtered)
                    .select("ngrams", "docIndex")
                    // flatten list of ngrams to individual ngram per row
                    // add extra column - 'stem', so we can merge 
                    // with stemmed dataframe (that contains unigrams)
                    .flatMap(row -> {
                        List<Tuple3<String, String, String>> result = new ArrayList<>();
                        WrappedArray<String> list = (WrappedArray<String>) row.getAs("ngrams");
                        scala.collection.Iterator iter = list.iterator();
                        while (iter.hasNext()) {
                            String ngram = (String) iter.next();
                            result.add(new Tuple3(ngram, row.getAs("docIndex"), ngram));
                        }
                        return result.iterator();

                    }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING())).toDF("word", "docIndex", "stem");
            ngrams.show(false);
        }
        
        // Flatten the filtered words into a row for each word. 
        // Need the docIndex column to reduce back to array of words
        Dataset<Row> flattened = filtered.flatMap(row -> {
            List<Tuple2<String,String>> result = new ArrayList<>();
            WrappedArray<String> list = (WrappedArray<String>)row.getAs("filtered");
            scala.collection.Iterator iter = list.iterator();
            while(iter.hasNext()) {
                result.add(new Tuple2((String)iter.next(), row.getAs("docIndex")));
            }           
            return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(),Encoders.STRING())
        ).toDF("word","docIndex");

        // Get the stem for each word (Stemmer will only work on a single word, not on an array of words 
        Stemmer stemmer = new Stemmer().setLanguage(language).setInputCol("word").setOutputCol("stem");
        Dataset<Row> stemmed = stemmer.transform(flattened);
        if (ngrams != null) {
            stemmed = stemmed.union(ngrams);
        }
        return stemmed;
        
    }
    /**
     * Create Dataset<Row> that contains all the info needed to save an NGram entity
     * groups the docStems dataframe by stem to get list of unstemmed words
     * @param docStems Docset<Row> - flattened map of all words and their 
     * associated stems.
     * columns:
     *        stem  
     *        word
     *          
     * @param vocabulary - String[] of stems used to create feature vector
     * @return Docset<Row> columns:
     *    stem - stemmed word
     *    unstemmed - list of unstemmed version of word found in docset
     *    index  - ngramIndex - maps to feature vectors
     *    word   - unstemmed word to use in UI
     *    count  - number of times stem appears in the document set
     */
    public static Dataset<Row> getNGramEntityValues(Dataset<Row> docStems, String[] vocabulary) {
           
        // Group words by stem in order to create the list of unstemmed words 
        // for each stem
        Dataset<Row> ngram = docStems.groupBy(col("stem")).agg(collect_set(col("word")).alias("unstemmed"),count("word").alias("count"));
        List vocabList = Arrays.asList(vocabulary);
        // Add index column from vocab[], and word = first unstemmed word in list
        // Now we have everything needed for NGram entity!
        StructType ngramSchema = ngram.schema();
        ngramSchema = ngramSchema.add("index", DataTypes.LongType);
        ngramSchema = ngramSchema.add("word", DataTypes.StringType);
        
     
      //  ngramSchema = ngramSchema.add("count", DataTypes.LongType);
        Dataset<Row> indexed = ngram.map(row -> {
            long index = vocabList.indexOf(row.getAs("stem"));  
            
            return RowFactory.create( 
                    row.getAs("stem"),   
                    row.getAs("unstemmed"),
                    row.getAs("count"),
                    index,
                   ((WrappedArray<String>)row.getAs("unstemmed")).head()
            );
        }, RowEncoder.apply(ngramSchema)).filter(col("index").gt(-1)).toDF();
        
        return indexed; 
        
    }
    
    public static void processDocuments(String mongoSetId, String classifierId) {

        SparkSession session = SparkSession
                .builder()
                .appName("Stemming Example")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.Document")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mydb.NGram")
                .getOrCreate();
        String language = "english";
        boolean bigrams = false;
        int minDocFrequency = 1;
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());
        
        // Get Document Text from Mongo 
        Dataset<Row> docCollection = MongoSpark.load(context).toDF().select("docIndex", "text", "mongoSetId");
        StructField[] schemafields = docCollection.schema().fields();
        System.out.println(schemafields[0]);
        docCollection.createOrReplaceTempView("myview");
        Dataset<Row> docs = session.sql("SELECT * FROM myview WHERE mongoSetId.oid ='" + mongoSetId + "'");
        
        // Analyze text to get feature Dataset and NGram Dataset
        Tuple2<Dataset<Row>, Dataset<Row>> result = analyzeText(docs, classifierId, language, bigrams, minDocFrequency);
        
        // Save NGram dataset to NGram Mongo Collection, with classifierId
        StructField oidStructField = StructFields.objectId("classifierId", true);
        final StructType oidSchema = DataTypes.createStructType(new StructField[] {oidStructField});
        Dataset<Row> ngrams = result._2;
        ngrams.printSchema();
        StructType ngramSchema = ngrams.schema();
        ngramSchema = ngramSchema.add(oidStructField);
      
        Dataset<Row> ngramDocs = ngrams.map(row -> {
            return RowFactory.create(                    
                    row.getAs("stem"),   
                    row.getAs("unstemmed"),
                    row.getAs("count"),
                    row.getAs("index"),
                    row.getAs("word"),
                    new GenericRowWithSchema(new String[] {classifierId}, oidSchema)
            );
        }, RowEncoder.apply(ngramSchema));
       
        // Note: have to do this filter to avoid a NullPointerException
        // when saving, even though
        // there are no rows in the dataset with classifierId = null!
        MongoSpark.save(ngramDocs.filter(col("classifierId").isNotNull()));
        // System.out.println("number of ngram rows with null classifierId: "+ngramDocs.filter(col("classifierId").isNull()).count());
  
        // Now save Feature vector to WordDocCount collection     
        Dataset<Row> features = result._1;
       
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "WordDocCount");
        WriteConfig writeConfig = WriteConfig.create(context).withOptions(writeOverrides);
        
        // Note: have to do this filter to avoid a NullPointerException, even though
        // there are no rows in the dataset with cId = null!
        MongoSpark.save(features.filter(col("cId").isNotNull()), writeConfig);
        //    System.out.println("number of feature rows with null cId: "+features.filter(col("cId").isNull()).count());
    }
    
    public static Tuple2<Dataset<Row>, Dataset<Row>> analyzeText(Dataset<Row> docText,String classifierId, String language, boolean ngramSize, int minDocFrequency) {
        String[] stopWords = StopWordsRemover.loadDefaultStopWords(language);
       
        Dataset<Row> docStems = getDocStems(docText, stopWords, language, ngramSize);
        System.out.println("docStems: ");
        docStems.show(false);
        Tuple2<Dataset<Row>, String[]> features = getDocFeatures(docStems, minDocFrequency, classifierId);
        Dataset<Row> ngrams = getNGramEntityValues(docStems, features._2);
        return new Tuple2(features._1, ngrams);
    }
    
    
    /**
     * Loads the csv file from the specified path using SparkContext.
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
