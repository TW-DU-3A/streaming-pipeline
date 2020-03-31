package com.tw.apps

import com.tw.apps.StationLocationUtils._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}

object StationLocationApp {

  def main(args: Array[String]): Unit = {

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    if (args.length != 2) {
      val message = "Two arguments are required: \"zookeeper server\" and \"application folder in zookeeper\"!"
      throw new IllegalArgumentException(message)
    }
    val zookeeperConnectionString = args(0)
    val zookeeperFolder = args(1)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start

    val kafkaBrokers = new String(zkClient.getData.forPath(s"$zookeeperFolder/kafkaBrokers"))

    val topic = new String(zkClient.getData.watched.forPath(s"$zookeeperFolder/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/checkpointLocation"))

    val dataLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))

    val spark = SparkSession.builder
      .appName("RawDataSaver")
      .getOrCreate()

    val readerStreamOptions = Map(
      ("kafka.bootstrap.servers", kafkaBrokers),
      ("subscribe", topic),
      ("startingOffsets", "latest"),
      ("failOnDataLoss", "false")
    )
    val writerOptions = Map(
      ("checkpointLocation", checkpointLocation),
      ("path", dataLocation)
    )

    val stream: DataStreamReader = spark.readStream
    run(stream, "kafka", "append", "parquet", readerStreamOptions, writerOptions)
      .start()
      .awaitTermination()
  }

  def run(sourceStream: DataStreamReader,
          sourceFormat: String,
          outputMode: String,
          outputFormat: String,
          readerStreamOptions: Map[String, String],
          writerOptions: Map[String, String]): DataStreamWriter[Row] = {
    sourceStream
      .createSource(sourceFormat, readerStreamOptions)
      .load()
      .addPayload()
      .writeStream
      .partitionByDate()
      .createSink(outputMode, outputFormat, writerOptions)
  }
}
