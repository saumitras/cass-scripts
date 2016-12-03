package datastax

import com.datastax.driver.core.{ColumnMetadata, Cluster}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object GetMeta extends App {
  val cluster = Cluster.builder().addContactPoint("10.0.10.210").build()
  val session = cluster.connect("aruba_aruba_podv1")

  val cols:List[ColumnMetadata] = session.getCluster.getMetadata.getKeyspace("aruba_aruba_podv1").getTable("event_tbl").getColumns.toList

  for(c <- cols) {
    println(c.getName + "  " + c.getType)
  }

  session.close()
  cluster.close()

}
