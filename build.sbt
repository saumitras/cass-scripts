name := "cassandraTest"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5" withSources(),
  "org.apache.cassandra" % "cassandra-clientutil" % "2.1.4" withSources(),
  "org.scala-lang.modules" %% "scala-pickling" % "0.10.1" withSources()
)

    