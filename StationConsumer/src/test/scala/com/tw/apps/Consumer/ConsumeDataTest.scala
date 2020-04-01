package com.tw.apps.Consumer

import java.nio.file.Files

import com.tw.apps.{DefaultFeatureSpecWithSpark, StationApp}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.streaming.Trigger

class ConsumeDataTest extends DefaultFeatureSpecWithSpark {
    ignore("Consume Data from kafka for San Francisco") {
        scenario("We have data in kafka topic, we should be able to read and get transformed data from it") {
            val zookeeperConnectionString = "zookeeper:2181"
            val retryPolicy = new ExponentialBackoffRetry(1000, 3)
            val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
            zkClient.start()
            val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))
            val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))
            val dataFrame = StationApp.getDataFromKafka(spark, stationKafkaBrokers, sfStationTopic)

            val query = dataFrame
                .writeStream
                .format("memory")
                .queryName("Output")
                .outputMode("append")
                .start()
            query.processAllAvailable()
            query.stop()
            val rows = spark.sql("select * from Output").collectAsList()
            Then("The data should have multiple rows")
            assert(rows.size() > 0)
        }
    }
}
