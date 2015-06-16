import AssemblyKeys._

name := "DecisiveCMS"

version := "0.1"

organization := "com.scoopwhoop"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
    "org.joda" % "joda-convert" % "1.7",
    "joda-time" % "joda-time" % "2.7",
    "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.4.0" % "provided",
    "org.apache.spark" % "spark-sql_2.10" % "1.4.0" % "provided",
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.0",
    "org.apache.cassandra" % "cassandra-clientutil" % "2.1.0" ,
    "org.apache.cassandra" % "cassandra-thrift" % "2.1.0",
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1",
    "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.4.0-M1")

assemblySettings

jarName in assembly := "dcms-assembly.jar"

assemblyOption in assembly :=
    (assemblyOption in assembly).value.copy(includeScala = false)