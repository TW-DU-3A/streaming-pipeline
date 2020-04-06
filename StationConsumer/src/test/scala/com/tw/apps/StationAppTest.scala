package com.tw.apps

;

import java.nio.file.Files
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

class StationAppTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  feature("run") {
    scenario("read from kafka stream and store as parquet") {
      Given("a data stream reader")
      val columnNames = List("bikes_available", "docks_available", "is_renting", "is_returning",
        "timestamp", "last_updated", "station_id", "name", "latitude", "longitude")
      val streamInput: MemoryStream[(Integer, Integer, Boolean, Boolean, Timestamp, Long, String, String, Double, Double)]
      = new MemoryStream[(Integer, Integer, Boolean, Boolean, Timestamp, Long, String, String, Double, Double)](1, spark.sqlContext)

      streamInput.addData(
        (8, 3, true, true, new Timestamp(1585526315000L), 1585546115, "983514094dd808b1604da2dcfc2d09af", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (17, 8, true, true, new Timestamp(1585526315000L), 1585546115, "98cf498d2fa09046f98abeb6a9e902ff", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277),
        (12, 15, true, true, new Timestamp(1585526315000L), 1585546115, "a730b0443ebed771ffc0c413ccc47eec", "Bryant St at 2nd St", 37.783171989315306, -122.39357203245163),
        (4, 11, true, true, new Timestamp(1585526315000L), 1585546115, "d0e8f4f1834b7b33a3faf8882f567ab8", "Harmon St at Adeline St", 37.849735, -122.270582))
      val inputDF = streamInput.toDS().toDF(columnNames: _*)

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)
      val dataPath = rootDirectory.toAbsolutePath.toString + "/data"
      val checkpointPath = rootDirectory.toAbsolutePath.toString + "/checkpoint"
      val inputStreamDataPath = rootDirectory.toAbsolutePath.toString + "/inputData"
      val inputStreamCheckpointPath = rootDirectory.toAbsolutePath.toString + "/inputCheckpoint"
      val writerOptions = Map(("checkpointLocation", checkpointPath), ("path", dataPath))

      StationApp
        .run(spark, inputDF, "append", "csv", writerOptions)
        .trigger(Trigger.Once()).start().processAllAvailable()

      //spark.read.csv(dataPath).show()

      streamInput.addData(
        (11, 0, true, true, new Timestamp(1586125644000l), 1586145444, "983514094dd808b1604da2dcfc2d09af", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (10, 1, true, true, new Timestamp(1586129244000l), 1586129244, "983514094dd808b1604da2dcfc2d09af", "Fountain Alley at S 2nd St", 37.33618830029063, -121.88927650451659),
        (8, 18, true, true, new Timestamp(1586125644000l), 1586145444, "98cf498d2fa09046f98abeb6a9e902ff", "Hubbell St at 16th St", 37.766482696439496, -122.39827930927277),
        (20, 7, true, true, new Timestamp(1586125644000l), 1586145444, "a730b0443ebed771ffc0c413ccc47eec", "Bryant St at 2nd St", 37.783171989315306, -122.39357203245163),
        (4, 11, true, true, new Timestamp(1586125644000l), 1586145444, "d0e8f4f1834b7b33a3faf8882f567ab8", "Harmon St at Adeline St", 37.849735, -122.270582))

      StationApp
        .run(spark, inputDF, "append", "csv", writerOptions)
        .trigger(Trigger.Once()).start().processAllAvailable()

      //val outputdf = spark.read.csv(dataPath)
      //outputdf.show
      assert(spark.read.csv(dataPath + "/station_id=983514094dd808b1604da2dcfc2d09af/year=2020/month=4/day=6").rdd.partitions.size === 1)
    }
  }
}
