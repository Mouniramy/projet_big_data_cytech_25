package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC_Taxi_Ingestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .getOrCreate()

    // Lecture depuis le Data Lake (Minio) 
    val df = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

    // Nettoyage : Validation des données selon le contrat 
    val cleanedDf = df.filter(
      col("passenger_count") > 0 && 
      col("trip_distance") > 0.0 &&
      col("fare_amount") > 0.0
    )

    // BRANCHE 1 : Sauvegarde en Parquet sur Minio pour ML [cite: 45, 46, 53]
    cleanedDf.write
      .mode(SaveMode.Overwrite)
      .parquet("s3a://nyc-processed/refined_taxi_data.parquet")

    // BRANCHE 2 : Ingestion vers PostgreSQL (Data Warehouse) [cite: 43, 49, 55]
    // Attention: Le sujet suggère de faire l'exercice 3 (création des tables) 
    // avant de lancer cette ingestion. [cite: 47, 49]
    /*
    cleanedDf.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/nyc_dw")
      .option("dbtable", "raw_trips")
      .option("user", "user")
      .option("password", "password")
      .mode(SaveMode.Append)
      .save()
    */

    println("Job terminé : Branche 1 exportée.")
    spark.stop()
  }
}