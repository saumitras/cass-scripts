package bloat

import java.io.{FileOutputStream, File, PrintWriter}
import java.text.SimpleDateFormat

import scala.pickling.binary._
import scala.pickling.Defaults._

import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.{ResultSet, SimpleStatement, Cluster, Row, DataType}
import scala.collection.JavaConversions._

case class DColumn(name:String, datatype:String, value:String)
case class DRow(row:List[DColumn])


object CassToFile {

  val QUERY = "SELECT * FROM del1"
  val FETCH_SIZE = 5
  val CASS_SEEDS = "127.0.0.1"
  val KEYSPACE = "export"
  val CF = "del1"
  val EXPORT_DIR = "/tmp/"
  val META = "/tmp/1"

  val storeSerialize = true
  val storePlainText = true

  ExportData.export(CASS_SEEDS, KEYSPACE, CF, QUERY, FETCH_SIZE, EXPORT_DIR, storeSerialize, storePlainText)

}


object ExportData {

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

  def export(cassSeeds:String, keyspace:String, cf:String, query:String, fetchSize:Int, exportDir:String, storeSerialized:Boolean, storePlainText:Boolean) = {

    val dataFile = exportDir + "/data"
    val ddlFile = exportDir + "/ddl"
    val textWriter = new TextWriter(dataFile + ".bin")
    val serializedWriter = new SerializedWriter(dataFile + ".txt")


    val cluster = Cluster.builder().addContactPoint(cassSeeds).build()
    val session = cluster.connect(keyspace)

    //save DDL
    val ddl = session.getCluster.getMetadata.getKeyspace(keyspace).getTable(cf).toString
    new PrintWriter(ddlFile) { write(ddl); close() }


    val stmt = new SimpleStatement(query)
    stmt.setFetchSize(fetchSize)
    val rs:ResultSet = session.execute(stmt)
    val iter = rs.iterator()

    while (!rs.isFullyFetched) {
      rs.fetchMoreResults()
      val row = iter.next()
      if (row != null) {
        var line = List[DColumn]()
        for (key <- row.getColumnDefinitions.asList().toList) {
          getValue(key, row) match {
            case Some(x) =>
              if(x.value != "null") {
                //println("Datatpe:" + x.datatype + " value:" + x.value)
                line :::= List(x)
              }
            case None =>
          }
        }

        println(line)

        if(storeSerialized) serializedWriter.write(line + "\n")
        if(storePlainText) textWriter.write(line)

      }
    }

    session.close()
    cluster.close()
  }


  def getValue(key:Definition, row:Row):Option[DColumn] = {

    if (key != null) {
      val col = key.getName
      val datatype = key.getType
      try {
        val v = if(datatype == DataType.text) row.getString(col)
          else if(datatype == DataType.timestamp) df.format(row.getDate(col))
          else if(datatype == DataType.cint) row.getInt(col).toString
          else if(datatype == DataType.bigint) row.getLong(col).toString
          else if(datatype == DataType.cdouble) row.getDouble(col).toString
          else if(datatype == DataType.uuid) row.getUUID(col).toString
          else if(datatype == DataType.cfloat) row.getFloat(col).toString
          else if(datatype == DataType.cboolean) row.getBool(col).toString
          else if(datatype == DataType.varchar) row.getString(col)
          else {
            println("Unhandled datatype " + datatype.getName)
            ""
          }
        Some(DColumn(col, datatype.getName.toString, v))

      } catch {
        case ex:Exception =>
          println(ex.getMessage)
          None
      }
    } else {
      None
    }
  }
}


class TextWriter(filePath:String) {

  var cache = ""
  var writeCounter = 1
  val writer = new PrintWriter(new File(filePath))

  def write(row:List[DColumn]): Unit = {
    val l = row.map(c => c.value).mkString(",")
    cache += l
    writeCounter += 1

    if(writeCounter % 1000 == 0) {
      writer.write(cache)
      cache = ""
    }
  }

  def close() = {
    writer.write(cache)
    writer.close()
  }

}


class SerializedWriter(filePath:String) {

  var cache = Vector[DRow]()
  var writeCounter = 1
  val writer = new PrintWriter(new File(filePath))

  val outputStream = new FileOutputStream(filePath)

  def write(l:List[DColumn]) = {
    cache +:= DRow(l)
    writeCounter += 1
    if(writeCounter % 1000 == 0) {
      cache.pickleTo(outputStream)
      cache = Vector[DRow]()
    }
  }

  def close() = {
    cache.pickleTo(outputStream)
    writer.close()
  }


}