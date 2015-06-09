#!/usr/bin/env bash
mvn clean
mvn package
java -jar target/avro_example-1.0-jar-with-dependencies.jar users.avro output