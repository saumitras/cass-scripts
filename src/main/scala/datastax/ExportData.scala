package datastax

import com.datastax.driver.core.Cluster

object ExportData extends App {

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("export")
  val rows = 4
  val ps = session.prepare(s"INSERT INTO t3 (par_num, id, f1, f2) VALUES (?,?,?,?)")

  for (id <- 0 until rows) {
    println(id)
    val data = ps.bind()
    if(id %2 == 0) data.setInt("par_num",8) else data.setInt("par_num",9)
    data.setString("id", "id" + id.toString)
    data.setString("f1",id.toString * 3)
    data.setString("f2",id.toString * 3)
    session.execute(data)
  }

  session.close()
  cluster.close()
}
