#!/usr/bin/env bash
mvn clean
mvn package
mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.HCatalogWordCount" -Dexec.args="golfnew testoutput"


