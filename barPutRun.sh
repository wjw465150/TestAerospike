#! /bin/sh

basedir=`dirname $0`
echo "BASE DIR:$basedir"
cd $basedir

java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.PutBenchMarkMain2 T1,T2,T3,T4

