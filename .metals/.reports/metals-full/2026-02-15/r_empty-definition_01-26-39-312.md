error id: file://<WORKSPACE>/ex01_data_retrieval/build.sbt:
file://<WORKSPACE>/ex01_data_retrieval/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -javaOptions.
	 -javaOptions#
	 -javaOptions().
	 -scala/Predef.javaOptions.
	 -scala/Predef.javaOptions#
	 -scala/Predef.javaOptions().
offset: 740
uri: file://<WORKSPACE>/ex01_data_retrieval/build.sbt
text:
```scala
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"

// Autoriser l'accès aux modules internes de Java pour Spark
run / fork := true
run / javaO@@ptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
```


#### Short summary: 

empty definition using pc, found symbol in pc: 