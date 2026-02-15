package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC_Taxi_Cleaning")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    // Lecture des données brutes depuis Minio
    val rawDf = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

    // Nettoyage (Branche 1) : Validation selon le contrat
    // On filtre les passagers <= 0 et les distances aberrantes [cite: 44]
    val cleanedDf = rawDf.filter(
      col("passenger_count") > 0 && 
      col("trip_distance") > 0 &&
      col("total_amount") > 0
    )

    // Sauvegarde de la Branche 1 dans Minio (pour le ML) [cite: 45, 46]
    cleanedDf.write
      .mode(SaveMode.Overwrite)
      .parquet("s3a://nyc-processed/refined_taxi_data.parquet")

    println("✅ Branche 1 : Données nettoyées et stockées dans nyc-processed.")

    // --- BRANCHE 2 : Ingestion vers PostgreSQL ---
    println("--- Lancement de la Branche 2 : Ingestion PostgreSQL ---")

    // Sélectionner et renommer les colonnes pour correspondre au schéma fact_trips
    // On ne garde que les données dont les IDs existent dans nos tables de dimensions
    val factTripsDf = cleanedDf
      .filter(col("VendorID").isin(1, 2))
      .filter(col("PULocationID").isin(1, 132, 138))
      .filter(col("DOLocationID").isin(1, 132, 138))
      .select(
        col("VendorID").as("vendor_id"),
        col("PULocationID").as("pickup_location_id"),
        col("DOLocationID").as("dropoff_location_id"),
        col("passenger_count"),
        col("trip_distance"),
        col("total_amount")
      )

    factTripsDf.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/nyc_dw")
      .option("dbtable", "fact_trips")
      .option("user", "user")
      .option("password", "password")
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Append) // On ajoute les données aux tables existantes
      .save()

    println("✅ Branche 2 terminée : Données injectées dans PostgreSQL.")

    spark.stop()
  }
}