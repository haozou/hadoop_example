#!/usr/bin/env bash
mvn clean
mvn package

mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.WordCount" -Dexec.args="-Dnode1=10.10.2.36 hdfs://10.10.2.36:8020/csv/account.csv /tmp/myout"


