package com.tw.apps

import com.tw.apps.StationLocationUtils.DataFramePayload
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, date_format}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}


class StationLocationUtilsTest extends FeatureSpec with GivenWhenThen with Matchers {
  val spark: SparkSession = SparkSession.builder
    .appName("Spark Test App")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  feature("DataFramePayload") {
    scenario("test add payload") {
      Given("some data")
      val value = "hey there how are you hello"
      val data = Seq(value).toDF("value")

      When("we split data by space")
      val dataFrameWithPayload = data.addPayload()

      Then("it should gives us all words")
      val date = date_format(current_date(), "yyyy-MM-dd")
      val expectedColumns = Array("raw_payload", "date")
      val expectedLines = List((value, date)).toDF(expectedColumns: _*)

      dataFrameWithPayload.collect() should contain theSameElementsAs expectedLines.collect()
    }
  }

}
