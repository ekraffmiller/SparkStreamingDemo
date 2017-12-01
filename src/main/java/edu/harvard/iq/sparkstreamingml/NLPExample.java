/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.sparkstreamingml;

import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.annotators.RegexTokenizer;
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfApproach;
import com.johnsnowlabs.nlp.annotators.ner.regex.NERRegexApproach;
import com.johnsnowlabs.nlp.annotators.ner.regex.NERRegexApproach$;
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronApproach;
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetectorModel;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author ellenk
 */
public class NLPExample {

    public static void main(String args[]) throws IOException {
        String nerCorpustest = "/Users/ellenk/src/spark-nlp/src/test/resources/ner-corpus/dict.txt";
        String posCorpusPath = "/Users/ellenk/src/spark-nlp/src/main/resources/anc-pos-corpus";
        // Turn off logging so it's easier to see console output
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        if (args.length < 1) {
            System.err.println("Usage: NLPExample <csv path> ");
            System.exit(1);
        }
        String csvPath = args[0];

        SparkSession session = SparkSession
                .builder()
                .appName("NLP Example")
                .getOrCreate();
        String language = "english";
        Dataset<Row> docText =  loadCSVText(session, csvPath);
        DocumentAssembler documentAssembler = new DocumentAssembler()
                .setInputCol("text");
        Dataset<Row> documents = documentAssembler.transform(docText).select(col("document"));

        documents.show(false);

      SentenceDetectorModel sentenceDetector = new SentenceDetectorModel();
      sentenceDetector.setInputCols(new String[]{"document"});
      sentenceDetector.setOutputCol("sentence"); 
    
      //  val sentenceDetector = new SentenceDetectorModel()
  //.setInputCols("document")
 // .setOutputCol("sentence")
 /*
 
 Notes for monday:
 Trying to add StopWords to Pipeline so the NER works better (stop assignning PER to stop words like "will".
 
 Tokenizer doesnt work as an input to stopwords because stopwords filter requires String input rather than Struct that is output by regex
 (See schema in comment below.)
 
 Also, the NER transformer requires sentence input, not tokens, so using the stop words filter would not fix the problem!
 
 Maybe better to either A) remove stop word like entries from dict.txt
 B) do filtering after processing, to remove entities that match stop words.
 
 Probably the former?
 
 
 */
        
RegexTokenizer regexTokenizer = new RegexTokenizer();
regexTokenizer.setOutputCol("token");
regexTokenizer.setInputCols(new String[]{"sentence"});
//StopWordsRemover stopWordsRemover = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("unfilteredtoken").setOutputCol("token");        

    

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{
            documentAssembler,
            sentenceDetector,
            regexTokenizer
                       
        });

        Dataset<Row> output = pipeline
                .fit(docText)
                .transform(docText);

        output.select("document").show(1,false);
        output.select("sentence").show(1,false);
        output.select("token").show(1,false);
        System.out.println(output.schema());

        String nerCorpusdict = "/Users/ellenk/src/spark-nlp/src/main/resources/ner-corpus/dict.txt";
        
        NERRegexApproach nerTagger = new NERRegexApproach(); 
        NERRegexApproach$.MODULE$.train(nerCorpusdict);
       
        nerTagger.setInputCols(new String[] {"sentence"});
        nerTagger.setOutputCol("ner");
        nerTagger.setCorpusPath(nerCorpusdict);
       
        Dataset<Row> nerTags = nerTagger.fit(sentenceDetector.transform(output)).transform(output);
        nerTags.select("ner","sentence").show(false);
        
                
  
      
     
        
        PerceptronApproach posTagger = new PerceptronApproach();
        posTagger.setInputCols(new String[] {"sentence", "token"});
        posTagger.setOutputCol("pos");
        posTagger.setCorpusPath(posCorpusPath);
         
        Dataset<Row> posTags  = posTagger
        .fit(output)
        .transform(output);
      //  posTags.select("pos").show(false);
        
           
    
       NerCrfApproach model = new NerCrfApproach();
       model.setInputCols(new String[]{"document", "token", "pos"});
    //    model.setLabelColumn("label");
        model.setMinEpochs(1);
        model.setMaxEpochs(3);
      //  model.setDatsetPath("src/test/resources/ner-corpus/test_ner_dataset.txt");
        model.setC0(34);
        model.setL2(3.0);
        model.setOutputCol("ner");
    //    Dataset<Row> nerTags = model.fit(posTags).transform(posTags);
     //   nerTags.select("ner").show(false);   
      
        //  System.out.println("bigrams only:");
        //  documents._2.filtesr(row -> row.getAs("stem").toString().contains(" ")).show();
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