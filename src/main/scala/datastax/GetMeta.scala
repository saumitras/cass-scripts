package datastax

import com.datastax.driver.core.{ColumnMetadata, Cluster}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object GetMeta {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("glassbeam")

  val cols:List[ColumnMetadata] = session.getCluster.getMetadata.getKeyspace("glassbeam").getTable("event_tbl").getColumns.toList

  for(c <- cols) {
    println(c.getName + "  " + c.getType)
  }

  session.close()
  cluster.close()

}
