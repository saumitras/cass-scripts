package bloat

import java.io.PrintWriter
import java.io.File
import com.datastax.driver.core.Cluster
import scala.util.Random

object DataSetCreator extends App {
  val rows = 5000000
  val fields = 6
  val x = 5
  val numPar = 10
  val start = new java.util.Date
  var cache = ""
  var counter = 1
  def write(l:String): Unit = {
    cache += l
    counter += 1
    if(counter == 5000) {
      writer.write(cache)
      cache = ""
      counter = 1
    }
  }

  val writer = new PrintWriter(new File("/tmp/file2"))

  for (id <- 1 to rows) {
    println(id)
    var row = List((id % 10).toString, id)
    for (n <- 0 until (fields + 1)) {
      row :::= List(randStr(4,1) * 5)
    }
    write(row.reverse.mkString(",") + "\n")
  }
  writer.write(cache)
  writer.close()
  println("Start= " + start + "   end= " + new java.util.Date)

  def randStr(len: Int, n:Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    val sb = new StringBuilder
    for (i <- 1 to len) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    if(n == 1)
      sb.toString
    else sb.toString + randStr(len,n-1)
  }


}
