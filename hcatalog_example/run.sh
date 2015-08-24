#!/usr/bin/env bash
mvn clean
mvn package
mvn exec:java -Dnode1=10.10.2.36 -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.HCatalogWordCount" -Dexec.args="acro_sample testoutput"

#mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.HCatalogExample" -Dexec.args="default.acro_sample"


