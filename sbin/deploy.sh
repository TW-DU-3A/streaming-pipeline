#!/usr/bin/env bash

set -xe

usage() {
    echo "Deployment script "
    echo "./deply.sh BASTION_PUBLIC_IP"
}

if [ $# -eq 0 ]; then
    usage
    exit 1
fi

BASTION_PUBLIC_IP=$1
COHORT="twdu3a"

echo "====Updating SSH Config===="

echo "
	User ec2-user
	IdentitiesOnly yes
	ForwardAgent yes
	DynamicForward 6789
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host emr-master.${COHORT}.training
    User hadoop

Host *.${COHORT}.training !bastion.${COHORT}.training
	ForwardAgent yes
	ProxyCommand ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ec2-user@${BASTION_PUBLIC_IP} -W %h:%p 2>/dev/null
	User ec2-user
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host bastion.${COHORT}.training
    User ec2-user
    HostName ${BASTION_PUBLIC_IP}
    DynamicForward 6789
" >> ~/.ssh/config

echo "====SSH Config Updated===="

echo "====Insert app config in zookeeper===="
scp ./zookeeper/seed.sh kafka.${COHORT}.training:/tmp/zookeeper-seed.sh
ssh kafka.${COHORT}.training '
set -e
export hdfs_server="emr-master.twdu3a.training:8020"
export kafka_server="kafka.twdu3a.training:9092"
export zk_command="zookeeper-shell localhost:2181"
sh /tmp/zookeeper-seed.sh
'
echo "====Inserted app config in zookeeper===="

echo "====Copy jar to ingester server===="
scp CitibikeApiProducer/build/libs/tw-citibike-apis-producer0.1.0.jar ingester.${COHORT}.training:/tmp/
echo "====Jar copied to ingester server===="

ssh ingester.${COHORT}.training '
set -e

function kill_process {
    query=$1
    pid=`ps aux | grep $query | grep -v "grep" |  awk "{print \\$2}"`

    if [ -z "$pid" ];
    then
        echo "no ${query} process running"
    else
        kill -9 $pid
    fi
}

station_information="station-information"
station_status="station-status"
station_san_francisco="station-san-francisco"
station_france="station-france"


echo "====Kill running producers===="

kill_process ${station_information}
kill_process ${station_status}
kill_process ${station_san_francisco}
kill_process ${station_france}

echo "====Runing Producers Killed===="

echo "====Deploy Producers===="

nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_information} --kafka.brokers=kafka.twdu3a.training:9092 1>/tmp/${station_information}.log 2>/tmp/${station_information}.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_san_francisco} --producer.topic=station_data_sf --kafka.brokers=kafka.twdu3a.training:9092 1>/tmp/${station_san_francisco}.log 2>/tmp/${station_san_francisco}.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_status} --kafka.brokers=kafka.twdu3a.training:9092 1>/tmp/${station_status}.log 2>/tmp/${station_status}.error.log &
nohup java -jar /tmp/tw-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_france} --producer.topic=station_data_fr --kafka.brokers=kafka.twdu3a.training:9092 1>/tmp/${station_france}.log 2>/tmp/${station_france}.error.log &

echo "====Producers Deployed===="
'


echo "====Configure HDFS paths===="
scp ./hdfs/seed.sh emr-master.${COHORT}.training:/tmp/hdfs-seed.sh

ssh emr-master.${COHORT}.training '
set -e
export hdfs_server="emr-master.twdu3a.training:8020"
export hadoop_path="hadoop"
sh /tmp/hdfs-seed.sh
'

echo "====HDFS paths configured==="


echo "====Copy Raw Data Saver Jar to EMR===="
scp RawDataSaver/target/scala-2.11/tw-raw-data-saver_2.11-0.0.1.jar emr-master.${COHORT}.training:/tmp/
echo "====Raw Data Saver Jar Copied to EMR===="

scp sbin/go.sh emr-master.${COHORT}.training:/tmp/go.sh

ssh emr-master.${COHORT}.training '
set -e

source /tmp/go.sh

echo "====Kill Old Raw Data Saver===="

kill_application "StationStatusSaverApp"
kill_application "StationInformationSaverApp"
kill_application "StationDataSFSaverApp"
kill_application "StationDataFRSaverApp"

echo "====Old Raw Data Saver Killed===="

echo "====Deploy Raw Data Saver===="

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationStatusSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.twdu3a.training:2181 "/tw/stationStatus" 1>/tmp/raw-station-status-data-saver.log 2>/tmp/raw-station-status-data-saver.error.log &

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationInformationSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.twdu3a.training:2181 "/tw/stationInformation" 1>/tmp/raw-station-information-data-saver.log 2>/tmp/raw-station-information-data-saver.error.log &

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationDataSFSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.twdu3a.training:2181 "/tw/stationDataSF" 1>/tmp/raw-station-data-sf-saver.log 2>/tmp/raw-station-data-sf-saver.error.log &

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationLocationApp --name StationDataFRSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-raw-data-saver_2.11-0.0.1.jar kafka.twdu3a.training:2181 "/tw/stationDataFR" 1>/tmp/raw-station-data-fr-saver.log 2>/tmp/raw-station-data-fr-saver.error.log &

echo "====Raw Data Saver Deployed===="
'


echo "====Copy Station Consumers Jar to EMR===="
scp StationConsumer/target/scala-2.11/tw-station-consumer_2.11-0.0.1.jar emr-master.${COHORT}.training:/tmp/

scp StationTransformerNYC/target/scala-2.11/tw-station-transformer-nyc_2.11-0.0.1.jar emr-master.${COHORT}.training:/tmp/
echo "====Station Consumers Jar Copied to EMR===="

scp sbin/go.sh emr-master.${COHORT}.training:/tmp/go.sh

ssh emr-master.${COHORT}.training '
set -e

source /tmp/go.sh


echo "====Kill Old Station Consumers===="

kill_application "StationApp"
kill_application "StationTransformerNYC"

echo "====Old Station Consumers Killed===="

echo "====Deploy Station Consumers===="

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationApp --name StationApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-station-consumer_2.11-0.0.1.jar kafka.twdu3a.training:2181 1>/tmp/station-consumer.log 2>/tmp/station-consumer.error.log &

nohup spark-submit --master yarn --deploy-mode cluster --class com.tw.apps.StationApp --name StationTransformerNYC --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 500M --conf spark.executor.memory=2g --conf spark.cores.max=1 /tmp/tw-station-transformer-nyc_2.11-0.0.1.jar kafka.twdu3a.training:2181 1>/tmp/station-transformer-nyc.log 2>/tmp/station-transformer-nyc.error.log &

echo "====Station Consumers Deployed===="
'
