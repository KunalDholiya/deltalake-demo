package org.deltalake.example

import org.apache.spark.sql.SparkSession

object AwsS3Read {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Kafka2DeltaStreaming")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")

    spark.sql("create table users using delta location 's3a://url-will-go-here-for-parquet-location'")
    spark.sql("select COUNT(*), state from users where group by state").show(10000)
  }
}
