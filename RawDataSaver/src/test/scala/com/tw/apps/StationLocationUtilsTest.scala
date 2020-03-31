package com.tw.apps

import java.nio.file.Files

import com.tw.DefaultFeatureSpecWithSpark
import com.tw.apps.StationLocationUtils._
import org.apache.spark.sql.streaming.Trigger

class StationLocationUtilsTest extends DefaultFeatureSpecWithSpark {

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
    scenario("test data with multiple partitions") {
      import org.apache.spark.sql.execution.streaming.MemoryStream

      Given("multiple records of data with two dates")
      val stream: MemoryStream[(String, String)] = new MemoryStream[(String, String)](1, spark.sqlContext)
      stream.addData(("acid", "2020-03-30"), ("base", "2020-03-31"), ("properties", "2020-03-31"))
      val writeStream = stream.toDS().toDF("value", "date").writeStream

      When("when partitioned")
      val partitionedByDate = writeStream.partitionByDate()

      Then("save the partitioned data to files")
      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val data = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpoint = rootDirectory.toAbsolutePath.toString + "/checkpoint"

      partitionedByDate
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", data)
        .trigger(Trigger.Once())
        .start()
        .processAllAvailable()

      Then("the number of partitions returned should be 2")
      assert(spark.read.parquet(data + "/date=2020-03-30").rdd.getNumPartitions === 1)
      assert(spark.read.parquet(data + "/date=2020-03-31").rdd.getNumPartitions === 1)
    }
  }
}
