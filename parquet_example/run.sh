#!/usr/bin/env bash
mvn clean
mvn package

mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.WordCount" -Dexec.args="hdfs://nameservice1/automation_test_data/parquet/golfnew_parquet/* /tmp/myout"


