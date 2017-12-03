
name := "SeekWell"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

//
//groupId: org.apache.spark
//artifactId: spark-core_2.11
//version: 2.2.0

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion)

//"org.apache.spark" %% "spark-mllib_2.10" % "2.2.0" % "provided"