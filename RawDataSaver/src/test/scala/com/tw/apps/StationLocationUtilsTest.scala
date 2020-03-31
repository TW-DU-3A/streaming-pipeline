package com.tw.apps

import java.nio.file.Files
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

import com.tw.apps.StationLocationUtils._
import org.apache.spark.sql.SparkSession

class StationLocationUtilsTest extends FeatureSpec with GivenWhenThen with Matchers {
  val spark: SparkSession = SparkSession.builder
    .appName("Spark Test App")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  feature("StationLocationDataFrame") {
    scenario("test add payload") {
      Given("some data")
      val value = "hey there how are you hello"
      val data = Seq(value).toDF("value")

      When("we add payload")
      val dataFrameWithPayload = data.addPayload()

      Then("returns the raw payload with new date column")
      val date = java.time.LocalDate.now.toString

      val expectedColumns = Array("raw_payload", "date")
      val expectedLines = List((value, date)).toDF(expectedColumns: _*)

      dataFrameWithPayload.collect() should contain theSameElementsAs expectedLines.collect()
    }
  }


  feature("StationLocationStreamWriter") {
    scenario("test data with single partition") {
      Given("some streaming data")
      import org.apache.spark.sql.execution.streaming.MemoryStream
      val input = new MemoryStream[String](42, spark.sqlContext)
      input.addData(Array("vivek"))

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val data = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpoint = rootDirectory.toAbsolutePath.toString + "/checkpoint"

      When("when partitioned")
      input.toDF().addPayload()
        .writeStream
        .partitionByDate()
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", data)
        .start()
        .processAllAvailable()


      Then("should return correct number of partitions")
      spark.read.parquet(data).rdd.getNumPartitions should be equals 1
    }

    scenario("test data with multiple partitions") {
      Given("some streaming data")
      import org.apache.spark.sql.execution.streaming.MemoryStream

      val recordFrame = new MemoryStream[(String, String)](1, spark.sqlContext)
      recordFrame.addData(("acid", "2020-03-30"), ("base", "2020-03-31"))

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val data = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpoint = rootDirectory.toAbsolutePath.toString + "/checkpoint"

      val inputStream = recordFrame.toDS().toDF("value", "date")

      When("when partitioned")
      inputStream
        .writeStream
        .partitionByDate()
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", data)
        .start()
        .processAllAvailable()


      Then("the number of partitions returned should be")
      spark.read.parquet(data).rdd.getNumPartitions should be equals 2
    }
  }
}
