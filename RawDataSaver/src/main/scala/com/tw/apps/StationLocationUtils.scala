package com.tw.apps

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{current_date, date_format}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}

object StationLocationUtils {

  implicit class StationLocationDataFrame(data: DataFrame) {

    def addPayload(): DataFrame = {
      data
        .selectExpr("CAST(value AS STRING) as raw_payload")
        .withColumn("date", date_format(current_date(), "yyyy-MM-dd"))
    }
  }

  implicit class StationLocationReader(stream: DataStreamReader) {

    def createSource(format: String, options: Map[String, String]): DataStreamReader = {
      stream
        .format(format)
        .options(options)
    }
  }

  implicit class StationLocationStreamWriter(stream: DataStreamWriter[Row]) {

    def partitionByDate(): DataStreamWriter[Row] = {
      stream.partitionBy("date")
    }

    def createSink(mode: String, format: String, options: Map[String, String]): DataStreamWriter[Row] = {
      stream
        .outputMode(mode)
        .format(format)
        .options(options)
    }
  }
}
