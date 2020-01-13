#!/bin/bash

curl -s https://api.github.com/repos/TileDB-Inc/TileDB-Spark/releases/latest | grep browser_download_url | cut -d '"' -f 4 | xargs wget -qi -

sudo mkdir -p /usr/lib/spark/jars &&
sudo cp -r tiledb-spark-metrics-*.jar /usr/lib/spark/jars