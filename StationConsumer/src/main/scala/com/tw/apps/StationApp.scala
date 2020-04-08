package com.tw.apps

import StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object StationApp {

  def getDataFromKafka(spark: SparkSession, stationKafkaBrokers: String, stationTopic: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", stationTopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
  }

  def main(args: Array[String]): Unit = {

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataNYC/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/checkpointLocation"))

    val outputLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/dataLocation"))

    val spark = SparkSession.builder
      .appName("StationConsumer")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "30")

    val nycStationDF = getDataFromKafka(spark, stationKafkaBrokers, nycStationTopic)
      .transform(nycStationStatusJson2DF(_, spark))

    val sfStationDF = getDataFromKafka(spark, stationKafkaBrokers, sfStationTopic)
      .transform(sfStationStatusJson2DF(_, spark))

    val stationDF = nycStationDF
      .union(sfStationDF)

    val writerOptions = Map(
      ("checkpointLocation", checkpointLocation),
      ("path", outputLocation)
    )

    run(spark, stationDF, "append", "parquet", writerOptions)
      .start()
      .awaitTermination()
  }

  def run(spark: SparkSession, stationDF: DataFrame, outputMode: String, format: String, writerOptions: Map[String, String]): DataStreamWriter[StationData] = {
    import spark.implicits._

    val seconds = "5 minute"
    val minutes = "10 minutes"

    val df1 = stationDF
      .withWatermark("timestamp", seconds)
      .groupBy(
        window($"timestamp", minutes),
        $"station_id"
      )
      .agg(
        last("timestamp").alias("timestamp"),
        last("bikes_available").alias("bikes_available"),
        last("docks_available").alias("docks_available"),
        last("is_renting").alias("is_renting"),
        last("is_returning").alias("is_returning"),
        last("last_updated").alias("last_updated"),
        last("name").alias("name"),
        last("latitude").alias("latitude"),
        last("longitude").alias("longitude")
      )
      .drop($"window")
      .as[StationData]

    df1
      .writeStream
      .createSink(outputMode, format, writerOptions)
  }
}
