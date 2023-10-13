#!/bin/bash
if [ -e /home/flink-example/data/result.txt ]; then
    rm -rf /home/flink-example/data/result.txt
    echo "delete"
fi

mvn -DskipsTest clean install &&
flink run -c App  ./target/flink-exmaple-1.0-SNAPSHOT-jar-with-dependencies.jar
