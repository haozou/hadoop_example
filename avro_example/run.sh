#!/usr/bin/env bash
mvn clean
mvn package
#java -jar target/avro_example-1.0-jar-with-dependencies.jar acro_sample.avro output
PROJECT_ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
LIB_JARS=$PROJECT_ROOT/libjars/avro-mapred-1.7.4-hadoop2.jar
mvn exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.mainClass="com.alpine.runner.App" -Dexec.args="/Datasets/avro/StudentActivity.snappy.avro /tmp/myout"
