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

    val nycStationDF = getDataFromKafka(spark, stationKafkaBrokers, nycStationTopic)
      .transform(nycStationStatusJson2DF(_, spark))

    val sfStationDF = getDataFromKafka(spark, stationKafkaBrokers, sfStationTopic)
      .transform(sfStationStatusJson2DF(_, spark))
    val stationDF = nycStationDF
      .union(sfStationDF)

    val writerOptions = Map(
      ("checkpointLocation", checkpointLocation),
      ("path", outputLocation),
      ("truncate", "false"),
      ("header", "true")
    )

    run(spark, stationDF, "complete", "overwriteCSV", writerOptions)
      .start()
      .awaitTermination()
  }

  def run(spark: SparkSession, stationDF: DataFrame, outputMode: String, format: String, writerOptions: Map[String, String]): DataStreamWriter[Row] = {
    import spark.implicits._
    stationDF
      .withWatermark(("timestamp"), "15 minutes")
      //.groupBy(col("station_id"))
      //.agg(max(col("timestamp")))
      .as[StationData]
      //.groupByKey(r => r.station_id)
      //.reduceGroups((r1, r2) => if (r1.last_updated > r2.last_updated) r1 else r2)
      //.map(_._2)
      .withColumn("year", year(col("timestamp").cast(DateType)))
      .withColumn("month", month(col("timestamp").cast(DateType)))
      .withColumn("day", dayofmonth(col("timestamp").cast(DateType)))
      .writeStream
      .createSink(outputMode, format, writerOptions)
      .partitionBy("station_id", "year", "month", "day")
  }
}
