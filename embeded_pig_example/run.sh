#!/usr/bin/env bash
mvn clean
mvn package
PROJECT_ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
echo $HCATJAR
LIB_JARS=${PROJECT_ROOT}/lib_jars/hcatalog-pig-adapter-0.13.0.2.1.7.0-784.jar,${PROJECT_ROOT}/lib_jars/hive-hcatalog-core-0.13.0.2.1.7.0-784.jar,${PROJECT_ROOT}/lib_jars/hive-metastore-0.13.0.2.1.7.0-784.jar,${PROJECT_ROOT}/lib_jars/libthrift-0.9.0.jar,${PROJECT_ROOT}/lib_jars/hive-exec-0.13.0.2.1.7.0-784.jar,${PROJECT_ROOT}/lib_jars/libfb303-0.9.0.jar,${PROJECT_ROOT}/lib_jars/jdo-api-3.0.1.jar,${PROJECT_ROOT}/lib_jars/slf4j-api-1.7.5.jar,${PROJECT_ROOT}/lib_jars/hive-webhcat-java-client-0.13.0.2.1.7.0-784.jar

mvn exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.mainClass="com.alpine.hadoop.pig.PigWithHcatalog" -Dexec.args="golfnew /tmp/pigwithhcatlog"

 
