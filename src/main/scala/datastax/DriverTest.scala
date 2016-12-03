package datastax

import com.datastax.driver.core.{ResultSet, Cluster}
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object DriverTest {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("glassbeam")

  val results:ResultSet = session.execute("SELECT mfr,prod FROM event_tbl limit 2")

  val cols = results.getColumnDefinitions.asScala.map(c => c.getName -> c.getType).toMap

  var resultList:List[Map[String,String]] = List()

  val newData = results.asScala.toList



  for(row <- results.asScala.toList) {
    var m = HashMap[String,String]()
    for((cname,dataype) <- cols) {
      //println(s"Fetching $cname")
      if(dataype.toString.toUpperCase == "VARCHAR" || dataype.toString.toUpperCase == "TEXT")
        m += (cname -> row.getString(cname))
      else if(dataype.toString.toUpperCase == "BIGINT")
        m += (cname -> row.getLong(cname).toString)
      else if (dataype.toString.toUpperCase == "INT")
        m += (cname -> row.getInt(cname).toString)
      else if(dataype.toString.toUpperCase == "TIMESTAMP")
        m += (cname -> row.getDate(cname).toString)
    }

    if(m.keys.size > 0)                                                                                                                                                                                                 {
      resultList ::= m
    }

  }

  println(resultList)

  session.close()
  cluster.close()

}
