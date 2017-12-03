/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


package WardsPack {

  import java.io.File
  import java.text.DateFormat.{LONG, getDateInstance}
  import java.util.{Date, Locale}

  import org.apache.spark.sql.{DataFrame, DataFrameReader}

  import scala.collection.mutable.ListBuffer
  import scala.io.Source

  object Builder {



    import spark.implicits._



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
      * Take collection of files and return the content as dataframe
      * Notice: it treats each file as textual row in DF
      * @param files List of files
      * @return Dataframe with files
      */
    def convertFilesToDataframe(files : List[File]) : DataFrame = {

//      var tmp = new ListBuffer[String]()
//      files.foreach(f => tmp+=(readFileContent(f.getAbsolutePath)))



      var tmp = new ListBuffer[Doc]()
      files.foreach(f => tmp+= Doc(f.getName,(readFileContent(f.getAbsolutePath))))

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


  }



}*/