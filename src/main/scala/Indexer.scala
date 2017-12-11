import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source
import scala.collection.mutable.Map

trait Indexer{

  val spark = SparkSession.
    builder().
    appName("SeekWell").
    master("local").
    getOrCreate()

  import spark.implicits._

  /**
    * Get List of all files
    * @param dir the relative path to the files folder
    * @return List object with files
    */
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      return d.listFiles.filter(_.isFile).toList
    } else {
      return List[File]()
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
    * @param file relative path to the file
    * @return String of file content
    */
  def readFileContent(file:File): String ={

    return Source.fromFile(file,"iso-8859-1").getLines.mkString
  }

  /**
    * Take collection of files and return the content as dataframe
    * Notice: it treats each file as textual row in DF
    * @param files List of files
    * @return Dataframe with files
    */
  def convertFilesToDataframe(files : List[File]) : DataFrame = {

    var tmp = Map[String, String]()
//    files.foreach(f => print(readFileContent(f)))
    files.foreach(f => tmp+=(f.getName -> readFileContent(f)))

    return tmp.toSeq.toDF("name","content").orderBy("name")
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
