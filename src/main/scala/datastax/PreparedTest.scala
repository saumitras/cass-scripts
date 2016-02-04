package datastax

import com.datastax.driver.core.{RegularStatement, BatchStatement, PreparedStatement, Cluster}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


import java.util.Date
case class Four(one:String, two:String)
object PreparedTest {

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("glassbeam")
  val f = Vector("212","2121")
  val ps = session.prepare("INSERT INTO messages (uid,tuid) VALUES (?,?)")

  var data = ps.bind()

  val value = 1421

  data.setString("uid","17")
  //data.setString("msg","sasa")
//  data.setMap[String, String]("context",Map("sasa" -> "sas", "bas" -> "rarr"))
  data.setUUID("tuid",com.datastax.driver.core.utils.UUIDs.timeBased())
  //data.setString("msg","sasasas")
  //data.setToNull("row")

  //println(data.preparedStatement().getQueryString)


  //data.bind(1.asInstanceOf[java.lang.Integer])

  //session.execute(ps bind ("1",null))

  println(data)
  session.execute(data)

  //val ps = session.prepare("INSERT INTO messages (uid, msg, row, ts) VALUES (?, ?, ?,?);")
  //session.execute(ps.bind("sam","212",0.asInstanceOf[java.lang.Integer],1421714821000L.asInstanceOf[java.util.Date]))

  //val batch = new BatchStatement()
  //batch.add(ps.bind("1","m1"))
  //batch.add(ps.bind("2","m1"))
  //batch.add(ps.bind("3","m1"))
  //batch.add(ps.bind("4","m1"))
  //session.execute(batch)


  /*
  val ps2 = session.prepare("NSERT INTO event_tbl (mfr,prod,sch,ec,sysid,obs_dt,tbl,obs_ts,row,severity,bundle_id,content,namespace,filename,type,begin_offset,obs_url,sys_host_name,obs_epoch,syslog_facility,syslog_hostname,syslog_version,syslog_appname,evt_epoch,syslog_procid,syslog_severity,evt_text) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

  session.execute(ps2.bind("a","a","a","a","a","a","a","a",0,"a","a",
    Jan 20 06:17:01 lcp rsyslogd: [origin software""","syslog",

    "/home/sam/projects/lcp/gbnewplatform/gbnewplatform-package/target/universal/gbnewplatform-package-4.7.0.11/permanent/syslog/syslog/syslog/CentOSv6.5/31-May-2015.17.+0530/messages.3/messages","EVENT",20,"/home/sam/projects/lcp/gbnewplatform/gbnewplatform-package/target/universal/gbnewplatform-package-4.7.0.11/permanent/syslog/syslog/syslog/CentOSv6.5/31-May-2015.17.+0530/messages.3/messages","messages",1421714821000L,0,"lcp","","rsyslogd",1421714821000L,0,0," rsyslogd was HUPed"
  ))
  */

  /*
  val ps2 = session.prepare("INSERT INTO event_tbl (mfr,prod,sch,ec,sysid,obs_dt,tbl,obs_ts,row) VALUES(?,?,?,?,?,?,?,?,?)")
  val batch = new BatchStatement()
  batch.add(ps2.bind(
    """a""","""a""","""a""","""a""","""a""",0,"""a""",0,0
  ))
  session.execute(batch)
*/
  session.close()
  cluster.close()

}
