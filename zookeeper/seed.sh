#!/bin/sh
echo $zk_command
$zk_command rmr /tw
$zk_command create /tw ''

$zk_command create /tw/stationDataNYC ''
$zk_command create /tw/stationDataNYC/topic station_data_nyc
$zk_command create /tw/stationDataNYC/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataNYC/checkpoints

$zk_command create /tw/stationInformation ''
$zk_command create /tw/stationInformation/kafkaBrokers $kafka_server
$zk_command create /tw/stationInformation/topic station_information
$zk_command create /tw/stationInformation/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationInformation/checkpoints
$zk_command create /tw/stationInformation/dataLocation hdfs://$hdfs_server/tw/rawData/stationInformation/data

$zk_command create /tw/stationStatus ''
$zk_command create /tw/stationStatus/kafkaBrokers $kafka_server
$zk_command create /tw/stationStatus/topic station_status
$zk_command create /tw/stationStatus/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationStatus/checkpoints
$zk_command create /tw/stationStatus/dataLocation hdfs://$hdfs_server/tw/rawData/stationStatus/data

$zk_command create /tw/stationDataSF ''
$zk_command create /tw/stationDataSF/kafkaBrokers $kafka_server
$zk_command create /tw/stationDataSF/topic station_data_sf
$zk_command create /tw/stationDataSF/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataSF/checkpoints
$zk_command create /tw/stationDataSF/dataLocation hdfs://$hdfs_server/tw/rawData/stationDataSF/data

$zk_command create /tw/stationDataFR ''
$zk_command create /tw/stationDataFR/kafkaBrokers $kafka_server
$zk_command create /tw/stationDataFR/topic station_data_fr
$zk_command create /tw/stationDataFR/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataFR/checkpoints
$zk_command create /tw/stationDataFR/dataLocation hdfs://$hdfs_server/tw/rawData/stationDataFR/data

$zk_command create /tw/output ''
$zk_command create /tw/output/checkpointLocation hdfs://$hdfs_server/tw/stationMart/checkpoints
$zk_command create /tw/output/dataLocation hdfs://$hdfs_server/tw/stationMart/data
