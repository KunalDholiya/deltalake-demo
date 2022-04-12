name := "DeltaLakeMVP"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("org.deltalake.example")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0",
  "io.delta" %% "delta-core" % "1.1.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.189",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.189",
  "com.amazonaws" % "aws-java-sdk" % "1.12.189",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
)