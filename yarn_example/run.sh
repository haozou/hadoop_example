#!/usr/bin/env bash
mvn clean
mvn package

mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.WordCount" -Dexec.args="-Dnode1=10.0.0.146 hdfs://10.0.0.146:8020/csv/account.csv /tmp/myout"


