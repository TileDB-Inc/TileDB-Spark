#!/bin/bash

sudo yum install -y cmake3 gcc g++ git &&
sudo ln -s /usr/bin/cmake3 /usr/bin/cmake &&
git clone https://github.com/TileDB-Inc/TileDB-Java.git -b 0.2.3 &&
cd TileDB-Java &&
./gradlew -PTILEDB_S3=ON -PTILEDB_VERBOSE=ON assemble &&
./gradlew -PTILEDB_S3=ON -PTILEDB_VERBOSE=ON publishToMavenLocal &&
cd ../ && rm -r TileDB-Java

git clone https://github.com/TileDB-Inc/TileDB-Spark.git -b master &&
cd TileDB-Spark &&
./gradlew assemble &&
./gradlew shadowJar &&
sudo mkdir -p /usr/lib/spark/jars &&
sudo cp -r build/libs/tiledb-spark-metrics-0.0.5.jar /usr/lib/spark/jars &&
cd ../
