package bloat

import java.io._
import scala.pickling.binary._
import scala.pickling.Defaults._

object PickleTest extends App {
  case class Col(name:String, datatype:String, value:String)
  case class Row(row:List[Col])

  //val data = scala.io.Source.fromFile("/tmp/" + filename).map(_.toByte).toArray

  //write
  val outputStream = new FileOutputStream("/tmp/p3")
  val col1 = Col("filename", "text", "/ebs/work/saumitra/file1")
  val col2 = Col("obs_url", "text", "/ebs/work/saumitra/obs1")
  val row1 = Row(List(col1, col2))
  val data = List(row1, row1)
  data.pickleTo(outputStream)
  data.pickleTo(outputStream)
  outputStream.close


  //read
  val inputStream = new FileInputStream("/tmp/p3")
  val iter = new Iterator[List[Row]] {
    val streamPickle = BinaryPickle(inputStream)
    override def hasNext: Boolean = inputStream.available > 0
    override def next: List[Row] = streamPickle.unpickle[List[Row]]
  }
  iter.foreach(x => println("H1 " + x))
/*

  def writeP() = {
    val pickedRow = row1.pickle

    val writer = new FileOutputStream("/tmp/p1")
    writer.write(pickedRow.value)
    writer.close()
  }

  def readP() = {

    try {
      val in = new FileInputStream("/tmp/p1")
      val br = new BufferedReader(new InputStreamReader(in))
      var line = ""
      while((line = br.readLine()) != null) {
        val unpickled = BinaryPickle(line.getBytes)
        val data = unpickled.unpickle[Row]
        println(data)
        line = br.readLine()
      }
    } catch {
      case ex:Exception =>
        ex.printStackTrace()
    }

  }

*/

}