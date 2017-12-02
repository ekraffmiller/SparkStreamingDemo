/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 *
 * @author ellenk
 */
public class CoreNLPExample {

    public static void main(String args[]) throws Exception {
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        if (args.length < 1) {
            System.err.println("Usage: CoreNLPExample <csv path> ");
            System.exit(1);
        }
        String csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("CoreNLP Example")
                .getOrCreate();
         
     //   Dataset<Row> docText = loadCSVText(session, csvPath);
        Dataset<Row> docText = loadSimpleSentence(session);
        
        // String serializedClassifier = "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";
        String serializedClassifier = "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz";
     
        //    
        // Test to view the NER annotations as an xml string
        StructType nlpSchema = new StructType();
        nlpSchema = nlpSchema.add("text", DataTypes.StringType);
        nlpSchema = nlpSchema.add("ann", DataTypes.StringType);
        docText.map(row -> {
            String str = ((String) row.getAs("text"));
            String classified = LazyInstantiator.getClassifier(serializedClassifier).classifyToString(str, "xml", true);

            return RowFactory.create(
                    str,
                    classified
            );
        }, RowEncoder.apply(nlpSchema)).select("ann").show(false);

        //This gets the entity label and filters for non-O entities.
        // TODO: aggregate sequential entities 
        // TODO: get counts for entities and sort by most frequent
        docText.flatMap(row -> {
            List<List<CoreLabel>> labels = LazyInstantiator.getClassifier(serializedClassifier).classify((String) row.getAs("text"));
            List<Tuple2<String, String>> result = new ArrayList<>();
            for (List<CoreLabel> lcl : labels) {
                for (CoreLabel cl : lcl) {
                    result.add(new Tuple2(cl.word(), cl.get(CoreAnnotations.AnswerAnnotation.class)));
                }
            }
            return result.iterator();
        },
                Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF().filter(row -> !row.getAs("value").equals("O"))
                .show( false);

      

        // Do Ner and pos in one pipeline
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos,lemma,ner");
        props.setProperty("ner.useSUTime", "0");
        props.setProperty("pos.model", "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
        props.setProperty("ner.model", "/Users/ellenk/Downloads/stanford-corenlp-models-current/edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz");

        // TODO, convert the annotated text with a flatMap - 
        // each row will be: token, POS, NER
        
         
        /* For POS: (Question - filter stop words first?)
              1. Do flatMap of sentences to get token and POS 
                  Output: token, POS 
              2. Filter out so we only have POS keys that we need for display. 
                    (Or do we need to group POS keys to more general?)
              3. Group tokens by value and POS key - for each grouping, record count.
                  Output:  token, POS, count
              4. For subset of POS keys - filter tokens with POS key, order by count, return top N tokens
                  Output: 
        
         For NER:
              1.  Before doing flat map, do processing to aggregate multitoken NERs.
                  Output: NER text, NER key  (for all non-O NERs)
        */
        
        
        Dataset<Annotation> annotationDF = docText.map(row -> {
            Annotation an = new Annotation((String) row.getAs("text"));
            LazyInstantiator.getStanfordCoreNLP(props).annotate(an);

            List<CoreMap> sentenceList = an.get(CoreAnnotations.SentencesAnnotation.class);
            if (sentenceList != null && !sentenceList.isEmpty()) {
                for (CoreMap sentence : sentenceList) {
                    /* System.out.println("The keys of the first sentenceList's CoreMap are:");
                System.out.println(sentence.keySet());
                System.out.println();
                System.out.println("The first sentenceList is:");
                System.out.println(sentence.toShorterString());
                System.out.println();
                System.out.println("The first sentenceList tokens are:"); */
                    for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        System.out.println("Value: " + token.get(CoreAnnotations.ValueAnnotation.class) + ", POS: "
                                + token.get(CoreAnnotations.PartOfSpeechAnnotation.class));

                    }
                }
            }
            return an;
        }, Encoders.bean(Annotation.class));

        
        

        annotationDF.foreach(ann -> LazyInstantiator.getStanfordCoreNLP(props).annotate(ann));

     
        session.stop();

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
    
    public static Dataset<Row> loadSimpleSentence(SparkSession session) {
          
         List<Row> simple = Arrays.asList(RowFactory.create("This is a very simple sentence by Ellen."));
         JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
         JavaRDD<Row> rdd = jsc.parallelize(simple);
          StructType schema = new StructType();
        schema = schema.add("text", DataTypes.StringType);
     return  session.createDataFrame(rdd.rdd(), schema);
      
    }

}

/*
StructType(
StructField(docIndex,IntegerType,true),
StructField(Item Type,StringType,true), 
StructField(Publication Year,IntegerType,true), 
StructField(Author,StringType,true), 
StructField(Title,StringType,true), 
StructField(Publication Title,StringType,true), 
StructField(text,StringType,true), 
StructField(document,
    ArrayType(StructType(
            StructField(annotatorType,StringType,true), 
            StructField(begin,IntegerType,false), 
            StructField(end,IntegerType,false), 
            StructField(result,StringType,true), 
            StructField(metadata,MapType(StringType,StringType,true),true)),
            true),
    true), 
StructField(sentence,
    ArrayType(StructType(
        StructField(annotatorType,StringType,true), 
        StructField(begin,IntegerType,false), 
        StructField(end,IntegerType,false),
        StructField(result,StringType,true), 
        StructField(metadata,MapType(StringType,StringType,true),true)),
        true),
    true), 
StructField(token,
    ArrayType(StructType(
        StructField(annotatorType,StringType,true), 
        StructField(begin,IntegerType,false), 
        StructField(end,IntegerType,false), 
        StructField(result,StringType,true), 
        StructField(metadata,MapType(StringType,StringType,true),true))
        ,true),
    true)
)
*/