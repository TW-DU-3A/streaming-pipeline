package com.tw.apps

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{current_date, date_format}
import org.apache.spark.sql.streaming.DataStreamWriter

object StationLocationUtils {

  implicit class StationLocationDataFrame(data: DataFrame) {

    def addPayload(): DataFrame = {
      data
        .selectExpr("CAST(value AS STRING) as raw_payload")
        .withColumn("date", date_format(current_date(), "yyyy-MM-dd"))
    }
  }

  implicit class StationLocationStreamWriter(stream: DataStreamWriter[Row]) {

    def partitionByDate(): DataStreamWriter[Row] = {
      stream.partitionBy("date")
    }
  }

}
