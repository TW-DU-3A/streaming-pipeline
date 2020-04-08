package com.tw.apps

;

import java.nio.file.Files
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.execution.streaming.MemoryStream


class StationAppTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  feature("run") {
    scenario("read from kafka stream and store as parquet") {
      Given("a data stream reader")
      val columnNames = List("bikes_available", "docks_available", "is_renting", "is_returning",
        "timestamp", "last_updated", "station_id", "name", "latitude", "longitude")

      val streamInput: MemoryStream[(Integer, Integer, Boolean, Boolean, Timestamp, Long, String, String, Double, Double)]
      = new MemoryStream[(Integer, Integer, Boolean, Boolean, Timestamp, Long, String, String, Double, Double)](1, spark.sqlContext)

      val window1_1 = Timestamp.valueOf("2020-04-06 05:00:00.00000")
      val window1_2 = Timestamp.valueOf("2020-04-06 05:03:00.00000")
      val window2_1 = Timestamp.valueOf("2020-04-06 05:10:00.00000")
      val window2_2 = Timestamp.valueOf("2020-04-06 05:14:00.00000")
      val window3_1 = Timestamp.valueOf("2020-04-06 05:26:00.00000")

      streamInput.addData(
        (1, 3, true, true, window1_1, 1585546115, "station_1", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (5, 11, true, true, window1_2, 1585546115, "station_1", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (3, 8, true, true, window1_1, 1585546115, "station_2", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277),

        (6, 2, true, true, window2_1, 1585546115, "station_1", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (1, 6, true, true, window2_2, 1585546115, "station_2", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277),

        (10, 0, true, true, window3_1, 1585546115, "station_2", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277)
      )

      When("we receive streaming data")
      val inputDF = streamInput.toDS().toDF(columnNames: _*)

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val dataPath = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpointPath = rootDirectory.toAbsolutePath.toString + "/checkpoint"
      val writerOptions = Map(("checkpointLocation", checkpointPath), ("path", dataPath), ("truncate", "false"))

      When("Multiple records for the same stations are received for the same window")
      val query = StationApp.run(spark, inputDF, "append", "parquet", writerOptions)
        .start()

      query
        .processAllAvailable()

      val resultdf = spark.read.parquet(dataPath).as[StationData]

      Then("The the most recent records per station is selected in the output")
      val expectedLines = List(
        (5, 11, true, true, window1_2, 1585546115, "station_1", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (6, 2, true, true, window2_1, 1585546115, "station_1", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (3, 8, true, true, window1_1, 1585546115, "station_2", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277),
        (1, 6, true, true, window2_2, 1585546115, "station_2", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277)
      ).toDF(columnNames: _*).as[StationData]

      //Assertion 1
      resultdf.collect() should contain theSameElementsAs expectedLines.collect()
    }
  }
}
