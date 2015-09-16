package bloat

import com.datastax.driver.core.Cluster

import scala.util.Random

object DummyLoader extends App {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("bloat")

  val rows = 1000
  val fields = 6
  val numPartitions = 10

  for (x <- 1 until 6) {
    val ps = session.prepare(s"INSERT INTO r4_x$x (par_num,id,f1,f2,f3,f4,f5,f6) VALUES (?,?,?,?,?,?,?,?)")
    for (id <- 0 until rows) {
      println(x + "-" + id)
      val data = ps.bind()
      val par_num = id % numPartitions
      data.setString("id", id.toString)
      data.setInt("par_num", par_num)
      for (n <- 1 until (fields + 1)) {
        data.setString("f" + n, randStr(20,x))
        //data.setToNull("f" + f)
      }
      session.executeAsync(data)
    }
  }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
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

  session.close()
  cluster.close()

}
