import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row



object Seeker extends Indexer{

  import spark.implicits._



  def main(args: Array[String]) :Unit = {

    val files= getListOfFiles(args(0))
    var df = convertFilesToDataframe(files)

    df.show()
    df.printSchema()

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("tokens")

    val tokenizedDf = tokenizer.transform(df)
//    val countTokens = udf { (words: Seq[String]) => words.length }

    tokenizedDf.show(10)

    val hashingTF = new HashingTF().
      setInputCol("tokens").setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(tokenizedDf)
    //  // alternatively, CountVectorizer can also be used to get term frequency vectors
    //  featurizedData.show()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("name", "features").show(false)

    rescaledData.show()

    //str.replace("[","").replace("]","").replace("(","").dropRight(1).split(",").map(_.toString.toDouble)

    val stripString = udf{ str:String => str.replace("[","").replace("]","").replace("(","").dropRight(1).split(",").map(_.toString.toDouble)}

    rescaledData.withColumn("fet",stripString($"features")).show()






    //  while(scala.io.StdIn.readLine("search>") != ":quit")
    //    {
    //      println("i'm here")
    //    }

  }

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

  */




}
//spark-submit --class Seeker --deploy-mode client --num-executors 4 --driver-memory 2g target/scala-2.11/seekwell_2.11-1.0.jar /Users/Ward/IdeaProjects/SeekWell/src/main/scala/Docs