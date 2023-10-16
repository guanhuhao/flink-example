#!/bin/bash
p=2
vertFile="/home/test_data/2/wiki_new.txt/flink/random-vertex.txt"
edgeFile="/home/test_data/2/wiki_new.txt/flink/random-edge.txt"
resuFile="/home/result"

if [ -e /home/flink-example/data/result.txt ]; then
    rm -rf /home/flink-example/data/result.txt
    echo "delete"
fi

mvn -DskipsTest clean install &&
flink run -c App /home/flink-example/target/flink-exmaple-1.0-SNAPSHOT-jar-with-dependencies.jar -p $p -v $vertFile  -e $edgeFile -r $resuFile
# flink run -c App  ./target/flink-exmaple-1.0-SNAPSHOT-jar-with-dependencies.jar

