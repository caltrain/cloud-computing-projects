#!/bin/bash

if [ $# -ne 5 ]; then
    echo Usage: [partition File][config][output file][num maps][num reducers]
    exit -1
fi


cp=$TWISTER_HOME/bin:.

for i in ${TWISTER_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

for i in ${TWISTER_HOME}/apps/*.jar;
  do cp=$i:${cp}
done

java -Xmx1024m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.samples.wordcount.WCMapReduce $1 $2 $3 $4 $5
