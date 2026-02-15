ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion"
  )

// Dépendances Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"

// Dépendances pour Minio (S3A)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"

// Dépendance pour PostgreSQL (Branche 2 du Data Warehouse)
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.2"

// Dépendance pour les tests unitaires (MainSpec.scala)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test

// Configuration pour éviter l'IllegalAccessError sur Mac
run / fork := true
run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"