package bloat

import com.datastax.driver.core.Cluster

object FileToCass {

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("export")

  val ps = session.prepare(s"INSERT INTO run2 (par_num,id,f1,f2,f3,f4,f5,f6,f7) VALUES (?,?,?,?,?,?,?,?,?)")

  scala.io.Source.fromFile("/home/sam/del/bloat/dummydata/file1").getLines().foreach(writeToCass)

  session.close()
  cluster.close()

  def writeToCass(l:String) = {
    val row = l.split(",")
    val data = ps.bind()

    val id = row(0)
    println(id)

    data.setString("id",id)
    data.setInt("par_num",row(1).toInt)
    for (n <- 1 to 7) {
      data.setString("f" + n, row(n+1))
    }
    session.execute(data)
  }



}
