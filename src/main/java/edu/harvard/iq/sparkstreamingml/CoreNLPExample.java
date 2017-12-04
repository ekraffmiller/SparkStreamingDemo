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
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

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

        /**
         * first step - run Annotation Pipeline on the docText dataset,
         * to get tokens that we use for NER and POS processing
         */
        Encoder annEncoder = Encoders.kryo(Annotation.class);      
        Dataset<Annotation> annotationDF = docText.map(row -> {
            System.out.println("!!!Annotating: "+ row.getAs("text"));
            Annotation an = new Annotation((String) row.getAs("text"));
            LazyInstantiator.getStanfordCoreNLP(props).annotate(an);
            return an;
        }, annEncoder);

        
      // TODO, convert the annotated text with a flatMap - 
        // each row will be: token, POS, NER
        
         
       
        /*
         For NER:
              1.  Before doing flat map, do processing to aggregate multitoken NERs.
                  Output: NER text, NER key  (for all non-O NERs)
        */
          

        annotationDF.foreach(ann -> {
                System.out.println("DATASET annotation is : "+ ann.toShorterString());
                 List<CoreMap> sentenceList = ann.get(CoreAnnotations.SentencesAnnotation.class);
            if (sentenceList != null && !sentenceList.isEmpty()) {
                for (CoreMap sentence : sentenceList) {
                    for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        System.out.println("Value: " + token.get(CoreAnnotations.ValueAnnotation.class) + ", POS: "
                                + token.get(CoreAnnotations.PartOfSpeechAnnotation.class));

                    }
                }
            }
        });
        /* For POS: (Question - filter stop words first?)
             -- Do flatMap of sentences to get token and POS 
                  Output: token, POS
             -- Filter stop words
             -- Filter out so we only have POS keys that we need for display. 
                    (Or do we need to group POS keys to more general?)
             -- Group tokens by value and POS key - for each grouping, record count.
                  Output:  token, POS, count
             -- For subset of POS keys - filter tokens with POS key, order by count, return top N tokens
                  Output:
        */
       
        Dataset<Row> posWords = annotationDF.flatMap(ann -> {
            List<Tuple2<String, String>> result = new ArrayList<>();
            List<CoreMap> sentenceList = ann.get(CoreAnnotations.SentencesAnnotation.class);
            if (sentenceList != null && !sentenceList.isEmpty()) {
                for (CoreMap sentence : sentenceList) {
                    for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        String word = token.get(CoreAnnotations.ValueAnnotation.class);
                        String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                        result.add(new Tuple2(word, pos));
                        //  System.out.println("Value: " + token.get(CoreAnnotations.ValueAnnotation.class) + ", POS: "
                        //        + token.get(CoreAnnotations.PartOfSpeechAnnotation.class));

                    }
                }
            }
            return result.iterator();
        }, Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("word", "pos");
        
        StopWordsRemover stopwords = new StopWordsRemover(); 
        List<String> stopWordList = Arrays.asList(stopwords.getStopWords());
        
        posWords = posWords.filter(row -> {return !stopWordList.contains((String)row.getAs("word"));}) // filter out stop words
                .filter(row -> filterUnwantedPOS(row) ) // filter out POS that we don't want to count
                .map(row -> mapPOS(row), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("word","pos")
                .groupBy(col("word"), col("pos")).count();
                
        
        // show top nouns ordered by frequency:
        posWords.filter(row -> row.getAs("pos").equals("VERB")).sort(col("count").desc())
               // .sort() // sort by frequency
                .show(50);
        
        session.stop();

    }

    
    private static boolean filterUnwantedPOS(Row row) {
        String pos =(String)row.getAs("pos");
        return pos.startsWith("V") || pos.startsWith("N") || pos.startsWith("RB") || pos.startsWith("J");
    }
    
    private static Tuple2<String,String> mapPOS( Row row) {
        String pos =(String)row.getAs("pos");
        String newPOS = pos;
        if (pos.startsWith("V")) {
            newPOS = "VERB";
        } else if (pos.startsWith("N")) {
            newPOS = "NOUN";
        } else if (pos.startsWith("RB")) {
            newPOS = "ADVERB";
        } else if (pos.startsWith("J")) {
            newPOS = "ADJECTIVE";
        }
        return new Tuple2(row.getAs("word"),newPOS);
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
          
         List<Row> simple = Arrays.asList(
                 RowFactory.create("The cold water filled the pond and reflected the gray clouds in the sky."),
                 RowFactory.create("Donald Trump's tweets that he posted this morning reflected badly on his judgement."),
                 RowFactory.create("The water from the refrigerator is nice and cold.  I filled my glass."));
         JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
         JavaRDD<Row> rdd = jsc.parallelize(simple);
          StructType schema = new StructType();
        schema = schema.add("text", DataTypes.StringType);
        Dataset<Row> text = session.createDataFrame(rdd.rdd(), schema);
     return  text;
      
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