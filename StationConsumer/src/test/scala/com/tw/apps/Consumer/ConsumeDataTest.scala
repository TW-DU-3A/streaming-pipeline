package com.tw.apps.Consumer

import java.nio.file.Files

import com.tw.apps.StationDataTransformation.{nycStationStatusJson2DF, sfStationStatusJson2DF}
import com.tw.apps.{DefaultFeatureSpecWithSpark, StationApp, StationData}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ConsumeDataTest extends DefaultFeatureSpecWithSpark {
  feature("Consume Data from kafka for San Francisco") {
    scenario("We have data in kafka topic, we should be able to read and get transformed data from it") {
      val zookeeperConnectionString = "zookeeper:2181"
      val retryPolicy = new ExponentialBackoffRetry(1000, 3)
      val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
      zkClient.start()
      val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))
      val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))
      val dataFrame = StationApp.getDataFromKafka(spark, stationKafkaBrokers, sfStationTopic)

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val dataPath = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpointPath = rootDirectory.toAbsolutePath.toString + "/checkpoint"
      When("Reading the streaming data")
      val query1 = dataFrame
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", checkpointPath)
        .option("path", dataPath)
        .start()
        .processAllAvailable()

      val df = spark.read.parquet(dataPath)
      //When("Performing transformations on the streaming data")

      //val inputDF = df.transform(sfStationStatusJson2DF(_, spark))
      //val inputDF = dataFrame.transform(sfStationStatusJson2DF(_, spark))

      //      val dataPath2 = rootDirectory.toAbsolutePath.toString + "/data2"
      //      val writerOptions = Map(("checkpointLocation", checkpointPath), ("path", dataPath2), ("truncate", "false"))
      //
      //      When("Multiple records for the same stations are received for the same window")
      //      val query = StationApp.run(spark, inputDF, "append", "parquet", writerOptions)
      //        .start()
      //
      //      query
      //        .awaitTermination(70000)
      //
      //val resultdf = spark.read.parquet(dataPath)
      //resultdf.show()

      Then("The data should have multiple rows")
      assert(df.count() > 0)

      //      val sfStationDF = dataFrame
      //        .transform(sfStationStatusJson2DF(_, spark))
      //
      //      val query = sfStationDF
      //        .writeStream
      //        //              .format("memory")
      //        //              .queryName("Output")
      //        //              .outputMode("append")
      //        .format("console")
      //        .option("truncate", false)
      //        .outputMode("update")
      //        .start()
      //
      //      query.processAllAvailable()
      //      query.stop()
      //      val rows = spark.sql("select * from Output").collectAsList()
      //      Then("The data should have multiple rows")
      //      assert(rows.size() > 0)
    }
  }
}
