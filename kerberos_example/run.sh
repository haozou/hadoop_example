#!/usr/bin/env bash
mvn clean
mvn package
mvn exec:java -Dprinciple=chorus/chorus.alpinenow.local@ALPINE -Dkeytab=./chorus.keytab -Duser=hao -Dnamenode=cdh5hakerberosnn.alpinenow.local -Dmetastore=cdh5hakerberosnn.alpinenow.local -Djava.security.krb5.realm=ALPINE -Djava.security.krb5.kdc=alpinenode4.alpinenow.local  -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.alpine.runner.KerberosExample"



