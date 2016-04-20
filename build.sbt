name := "cassandra-ec2multiregion-scala"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0"
)
