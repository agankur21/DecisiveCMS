import AssemblyKeys._



name := "DecisiveCMS"

version := "0.1"

organization := "com.scoopwhoop"

scalaVersion := "2.10.4"

resolvers ++= Seq(
    "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases",
    "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
    Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
    //Date time dependencies
    "org.joda" % "joda-convert" % "1.7",
    "joda-time" % "joda-time" % "2.7",
    
    //Json Depedencies  
    "net.liftweb" % "lift-json_2.10" % "2.5.1",
    
    //Spark depedencies
    "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.4.0",
    "org.apache.spark" % "spark-sql_2.10" % "1.4.0" ,
    "org.apache.spark" %% "spark-streaming" % "1.4.0",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0",
    
    //Cassandra Dependencies
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
    "org.apache.cassandra" % "cassandra-clientutil" % "2.1.5" ,
    "org.apache.cassandra" % "cassandra-thrift" % "2.1.5",
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1",
    "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.4.0-M1",
    
    //NLP Dependencies
    "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1")



assemblySettings

jarName in assembly := "dcms-assembly.jar"

assemblyOption in assembly :=
    (assemblyOption in assembly).value.copy(includeScala = false)

mergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case "log4j.properties"                                  => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf"                                    => MergeStrategy.concat
    case _                                                   => MergeStrategy.first
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
