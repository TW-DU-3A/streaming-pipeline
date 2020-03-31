package com.tw.apps

import java.nio.file.Files

import com.tw.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.streaming.Trigger

class StationLocationAppTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  feature("run") {
    scenario("read from kafka stream and store as parquet") {
      Given("a data stream reader")
      val data = List("acid", "base", "properties")
      val columns = List("value")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val dataPath = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpointPath = rootDirectory.toAbsolutePath.toString + "/checkpoint"
      val inputStreamDataPath = rootDirectory.toAbsolutePath.toString + "/inputData"
      val inputStreamCheckpointPath = rootDirectory.toAbsolutePath.toString + "/inputCheckpoint"
      val readerStreamOptions = Map(("path", inputStreamDataPath), ("checkpointLocation", inputStreamCheckpointPath))
      val writerOptions = Map(("checkpointLocation", checkpointPath), ("path", dataPath))

      When("we have data for streaming")
      data.toDS().toDF(columns: _*).write.parquet(inputStreamDataPath)


      When("run the job")
      val stream = spark.readStream.schema(spark.read.parquet(inputStreamDataPath).schema)
      StationLocationApp
        .run(stream, "parquet", "append", "parquet", readerStreamOptions, writerOptions)
        .trigger(Trigger.Once()).start().processAllAvailable()

      Then("should save the data to files")
      val date = java.time.LocalDate.now.toString
      val expectedColumns = Array("raw_payload", "date")
      val expectedLines = List(("acid", date), ("base", date), ("properties", date)).toDF(expectedColumns: _*)

//      spark.read.parquet(dataPath + "/date=" + date).collect() should contain theSameElementsAs expectedLines.collect()
      assert(spark.read.parquet(dataPath + "/date=" + date).rdd.getNumPartitions === 1)

    }
  }
}
