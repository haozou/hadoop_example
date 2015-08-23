#!/usr/bin/env bash
mvn clean
mvn package
mvn exec:java -Djava.security.krb5.realm=ALPINE -Djava.security.krb5.kdc=alpinenode4.alpinenow.local  -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.KerberosExample" -Dexec.args="-Dnode1=cdh5hakerberosnn.alpinenow.local acro_sample testoutput"

#mvn exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.HCatalogExample" -Dexec.args="default.acro_sample"


