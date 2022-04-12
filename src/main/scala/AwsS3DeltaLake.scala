package org.deltalake.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, StringType, StructType}

object AwsS3DeltaLake {
  def main(args: Array[String]): Unit ={

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Kafka2DeltaStreaming")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user-information")
      .option("startingOffsets", "latest")
      .option("includeHeaders", "true")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("id",StringType)
      .add("name",StringType)
      .add("email",StringType)
      .add("phone",StringType)
      .add("address",StringType)
      .add("city",StringType)
      .add("state",StringType)
      .add("zip",StringType)
      .add("county",StringType)
      .add("company",StringType)
      .add("jobTitle",StringType)
      .add("date",DateType)

    val personDF = df1.select(from_json(col("value"), schema).as("data"))
      .select("data.*")


    val bucketPathParquet = "s3a://url-will-go-here-for-parquet-location"
    val bucketPathCheckpoint = "s3a://url-will-go-here-for-checkpoint-location"

    val query = personDF.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", bucketPathCheckpoint)
      .trigger(Trigger.ProcessingTime("120 seconds"))
      .start(bucketPathParquet)

    query.awaitTermination()
  }
}
