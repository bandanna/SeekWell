//import WardsPack._
import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession,DataFrameReader}

import scala.io.Source
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer,HashingTF, IDF }

import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

import java.text.DateFormat.{LONG, getDateInstance}
import java.util.{Date, Locale}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object Seeker extends App{

  val spark = SparkSession
    .builder()
    .appName("SeekWell")
    .master("local")
    .getOrCreate()

  import spark.implicits._


  /**
    * Get List of all files
    * @param dir the relative path to the files folder
    * @return List object with files
    */
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /**
    * Case class made to structure sequential objects storing name and content of files
    * @param name
    * @param content
    */
  case class Doc(name: String, content: String)

  /**
    *
    * Gets filename and its content as string
    * @param filePath relative path to the file
    * @return String of file content
    */
  def readFileContent(filePath:String): String ={

    return Source.fromFile(filePath).getLines.mkString
  }

  /**
    * Take collection of files and return the content as dataframe
    * Notice: it treats each file as textual row in DF
    * @param files List of files
    * @return Dataframe with files
    */
  def convertFilesToDataframe(files : List[File]) : DataFrame = {

    print("heeerrrreeeee")

    var tmp = new ListBuffer[Doc]()
    print("heeerrrreeeee22222")

    files.foreach(f => print(readFileContent(f.getAbsolutePath)))

//    files.foreach(f => tmp+= Doc(f.getName,(readFileContent(f.getAbsolutePath))))

    print("heeerrrreeeee333333")


    return tmp.toSeq.toDF().orderBy("name")
  }

  /**
    * Takes dataframe and columns and retrieves a slice
    * @param df the dataframe to slice
    * @param cols the columns to retrieve
    * @return new dataframe with the required columns
    */
  def sliceDF(df: DataFrame, cols: String*) : DataFrame= {
    return df.select(cols.head, cols.tail: _*)
  }


  var files= getListOfFiles("src/main/scala/Docs")

  if (files == null)
    {
      print("this is null")
    }


  var df = convertFilesToDataframe(files)


  df.show()
  df.printSchema()
  /*

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("tokens")

    val tokenized_df = tokenizer.transform(df)
    val countTokens = udf { (words: Seq[String]) => words.length }

    tokenized_df.show(10)
    */

//  tokenized_df.select("content", "tokens")
//    .withColumn("tokens_count", countTokens(col("tokens"))).show(false)

//  tokenized_df.show(false)

/* // THIS WOULD KEEP MANY SINGLE LETTERS, overall regular tokenizer is better for the test dataset
  val regexTokenizer = new RegexTokenizer()
    .setInputCol("content")
    .setOutputCol("tokens")
    .setPattern("\\W")
  val regTokenized=regexTokenizer.transform(df)

  regTokenized.show(false)
*/


/*
  val hashingTF = new HashingTF()
    .setInputCol("tokens").setOutputCol("rawFeatures")


  val featurizedData = hashingTF.transform(tokenized_df)
//  // alternatively, CountVectorizer can also be used to get term frequency vectors
//  featurizedData.show()

  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)


  val rescaledData = idfModel.transform(featurizedData)

  rescaledData.select("name", "features").show(false)

  rescaledData.show()
  */

//  while(scala.io.StdIn.readLine("search>") != ":quit")
//    {
//      println("i'm here")
//    }


}
//spark-submit --class Seeker --deploy-mode client --num-executors 4 --driver-memory 4g target/scala-2.11/seekwell_2.11-1.0.jar