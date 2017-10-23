import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.writer._

import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.ml.feature.{ CountVectorizer, CountVectorizerModel, Tokenizer, StopWordsRemover, BucketedRandomProjectionLSH }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.Pipeline

import org.apache.spark.mllib.linalg.distributed.{ MatrixEntry, RowMatrix }

import breeze.linalg.norm
import org.apache.spark.mllib.linalg.{ SparseVector, Vectors, DenseVector }

import org.apache.spark.mllib.feature.Normalizer

import java.io.File

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row

import Utilities._


object TFIDContentBaseRecApp {

  def main(args: Array[String]) {
    
    var givenIndex = 1
    
    args.sliding(1, 2).toList.collect {
      case Array("--givenIndex", argGivenIndex: String) => givenIndex = argGivenIndex.toInt
    }

    //-------------------Data Praparation Part---------------------//
    //Step-1
    //Spark and SQL Context preparation
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("local")
      .set("spark.testing.memory", "471859200")

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //Step-2
    //Read csv file into DataFrame
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/sample-data-pl.csv")

    
    //Step-3
    //Remove all double quotes from text
    val removeDoubleQuotes = udf((x: String) => x.replace("\"", ""))
    val rawDoc = df.withColumn("description", removeDoubleQuotes($"description"))

    
    //Step-4
    //Process of taking text and breaking it into individual terms
    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("rawDesc")

    //Step-5
    //Takes as input a sequence of strings (the output of a Tokenizer) and drops all the stop words from the input sequences.
    val stopsRemover = new StopWordsRemover().setInputCol("rawDesc").setOutputCol("tokens")
    val customStops = sc.textFile("src/main/resources/stop-words-pl.csv").collect() 
    stopsRemover.setStopWords(stopsRemover.getStopWords union customStops union Array(" ", "", "\"", "-"))

    //Step-6
    //Pipeline combine multiple algorithms into a single workflow
    //Execute pipline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopsRemover))
    val docDF = pipeline.fit(rawDoc).transform(rawDoc)
    
    //Step-7
    //Prepare TFIDF input document by extraction tokens RDD
    val documents = docDF.select("tokens").as[Seq[String]].rdd

    
    //-------------------Predicion Part---------------------//
    
    //Step-1
    //TFIDF matrix calculation
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    val tfidfmatrix = tfidf.collect()
    
    //Step-2
    //Get tokens vector for interesting us document
    val givenDocumentVector = tfidfmatrix(givenIndex - 1)

    //Step-3
    //dot product of two normalized TF-IDF vectors is the cosine similarity of the vectors
    val normalizer1 = new Normalizer()
    val cosineSimilarity = tfidf.map(vect => {
      val v1 = normalizer1.transform(givenDocumentVector)
      val v2 = normalizer1.transform(vect)
      dot(v1, v2)
    })
    
    //Step-4
    //zipWithIndex function join similarity with index
    //in map index is recalculating to get line number
    val output = cosineSimilarity.zipWithIndex().map(r => (r._2 + 1, r._1))
    
    //Step-5
    //Join input DF and Output and show in descending order
    //First on list will be given dcoument with similarity==1
    val out = output.toDF("id","similarity").join(df,"id").orderBy($"similarity".desc)
    
    println(s"Similar products for index $givenIndex are...\n")
    out.show()
    
    sc.stop()

  }

}