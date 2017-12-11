import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{DenseVector, SparseMatrix, SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.collection.mutable.ListBuffer

// move to the main class
import org.apache.log4j.Logger
import org.apache.log4j.Level




object Seeker extends Indexer{

  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(args: Array[String]) :Unit = {

    val files= getListOfFiles(args(0))
//    val files= getListOfFiles("/Users/Ward/IdeaProjects/SeekWell/src/main/scala/Docs")
    val df = convertFilesToDataframe(files)

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("tokens")
    val wordsData = tokenizer.transform(df)

    val hashingTF = new HashingTF().setInputCol("tokens").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
//    rescaledData.select("name", "features").show()
    rescaledData.show()
    rescaledData.printSchema()

    rescaledData.select("rawFeatures", "features").show(false)

    val rowsDf = rescaledData.select("rawFeatures")

    //=====================

//    var mylist = ListBuffer[Array[Double]]()
    val mylist = ListBuffer.empty[Array[Double]]
//    scala.collection.mutable.ListBuffer.empty[Int]


    rescaledData.rdd.foreach{
      x => {
        var tmp = Array[Double]()
        tmp = x.get(3).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense.toArray
        println()
        mylist.+=:(tmp)
        //        println(mylist.length)
      }
    }

    var lst = scala.collection.mutable.ListBuffer.empty[List[Double]]
    for(vector <- rescaledData.select("rawFeatures").rdd)
    {
      var te = vector.get(0).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense.toArray.toList
      println(te.toString())
      lst.append(te)
    }

    def convertVector =
      for{ vector <- rescaledData.select("rawFeatures").rdd}
        yield {
          lst+=vector.get(0).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense.toArray.toList

        }


    var jkk =convertVector
    var l =jkk.collect().toList //List(ListBuffer(List(Double)))




    lst.foreach(println)

        //        val tmp = x.get(3).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense.toArray
//        println(tmp.toList.mkString(","))
        //        println(tmp.getClass)

    mylist.foreach(println)

//    tmp = rescaledData.rdd.map(row =>  IndexedRow(row.get(3).asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toDense))
//    frequencyVectors.map(lambda vector: DenseVector(vector.toArray()))



//    val pred_ = IndexedRowMatrix(rescaledData.rdd.map( x => IndexedRow(x.get(0),x.get(1)))).toBlockMatrix().transpose().toIndexedRowMatrix()
//    pred_sims = pred.columnSimilarities()


/*    val vectorizer = new HashingTF().setInputCol("tokens").setOutputCol("vectors")
    val lsh = new MinHashLSH().setInputCol("vectors").setOutputCol("lsh")
    val pipeline = new Pipeline().setStages(Array(tokenizer, vectorizer, lsh))

    var db = wordsData.select("content")

    val query = Seq("chiang mai in thailand").toDF("content")
    val model = pipeline.fit(db)

    val dbHashed = model.transform(db)
    val queryHashed = model.transform(query)

    model.stages.last.asInstanceOf[MinHashLSHModel].
      approxSimilarityJoin(dbHashed, queryHashed, 100).show*/


  }





}
//spark-submit --class Seeker --deploy-mode client --num-executors 4 --driver-memory 2g target/scala-2.11/seekwell_2.11-1.0.jar /Users/Ward/IdeaProjects/SeekWell/src/main/scala/Docs