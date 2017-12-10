import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.Pipeline




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

//    val stripString = udf{ str:String => str.replace("[","").replace("]","").replace("(","").dropRight(1).split(",").map(_.toString.toDouble)}
//
//    rescaledData.withColumn("fet",stripString($"features")).show()


    val vectorizer = new HashingTF().setInputCol("tokens").setOutputCol("vectors")
    val lsh = new MinHashLSH().setInputCol("vectors").setOutputCol("lsh")


    val pipeline = new Pipeline().setStages(Array(tokenizer, vectorizer, lsh))

    var db = tokenizedDf.select("content")

    val query = Seq("chiang mai in thailand").toDF("content")
    val model = pipeline.fit(db)

    val dbHashed = model.transform(db)
    val queryHashed = model.transform(query)

    model.stages.last.asInstanceOf[MinHashLSHModel].
      approxSimilarityJoin(dbHashed, queryHashed, 100).show



    //  while(scala.io.StdIn.readLine("search>") != ":quit")
    //    {
    //      println("i'm here")
    //    }

  }




/*

  */




}
//spark-submit --class Seeker --deploy-mode client --num-executors 4 --driver-memory 2g target/scala-2.11/seekwell_2.11-1.0.jar /Users/Ward/IdeaProjects/SeekWell/src/main/scala/Docs