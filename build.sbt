name := """hadoop-exam"""

version := "1.0"

scalaVersion := "2.11.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.mrunit" % "mrunit" % "1.0.0" % "test" classifier "hadoop2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.7.1"

libraryDependencies += "com.jamonapi" % "jamon" % "2.81"
//libraryDependencies += "io.reactivex" % "rxscala_2.11" % "0.25.0"
libraryDependencies += "org.apache.hive.hcatalog" % "hive-hcatalog-streaming" % "1.2.1"
libraryDependencies += "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "1.2.1"


libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.8.1"


mainClass in Compile := Some("edoudkin.hadoop.p3.P3AppDriver")
exportJars := true




// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"



fork in run := true