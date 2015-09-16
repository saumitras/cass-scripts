package bloat

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.{ResultSet, SimpleStatement, Cluster, Row, DataType}
import scala.collection.JavaConversions._

object CassToFile extends App {

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("export")

  val writer = new PrintWriter(new File("/tmp/cass_export1"))
  var cache = ""
  var counter = 1

  val stmt = new SimpleStatement("SELECT * FROM run1")
  stmt.setFetchSize(1000  )
  val rs:ResultSet = session.execute(stmt)
  val iter = rs.iterator()
  while (!rs.isFullyFetched()) {
    rs.fetchMoreResults()
    val row = iter.next()
    if (row != null) {
      var line = List[String]()
      for (key <- row.getColumnDefinitions().asList().toList) {
        val value = getValue(key, row)
        line :::= List(value)
      }
      println(counter)
      write(line.reverse.mkString(",") + "\n")
    }
  }

  writer.write(cache)
  writer.close()
  session.close()
  cluster.close()


  def write(l:String): Unit = {
    cache += l
    counter += 1
    if(counter % 1000 == 0) {
      writer.write(cache)
      cache = ""
    }
  }

  def getValue(key:Definition, row:Row) = {
    var str = ""

    if (key != null) {
      val col = key.getName()
      try {
        if (key.getType() == DataType.cdouble()) {
          str = row.getDouble(col).toString()
        } else if (key.getType() == DataType.cint()) {
          str = new Integer(row.getInt(col)).toString()
        } else if (key.getType() == DataType.uuid()) {
          str = row.getUUID(col).toString()
        } else if (key.getType() == DataType.cfloat()) {
          str = row.getFloat(col).toString()
        } else if (key.getType() == DataType.timestamp()) {
          str = row.getDate(col).toString()
          val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
          str = fmt.format(row.getDate(col))
        } else {
          str = row.getString(col);
        }
      } catch {
        case ex:Exception =>
          str = ""
      }
    }

    str

  }

}
