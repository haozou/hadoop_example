#!/usr/bin/env bash
mvn clean
mvn package
mvn exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.mainClass="com.alpine.runner.App" -Dexec.args="/Datasets/avro/StudentActivity.snappy.avro /tmp/myout"
