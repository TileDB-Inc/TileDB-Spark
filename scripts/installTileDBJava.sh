#!/bin/bash
if [ -d "TileDB-Java" ]; then
  rm -r ./TileDB-Java
fi
git clone https://github.com/TileDB-Inc/TileDB-Java.git -b 0.2.2
pushd TileDB-Java
git checkout master
./gradlew -PTILEDB_S3=ON -PTILEDB_VERBOSE=ON assemble --info
./gradlew -PTILEDB_S3=ON -PTILEDB_VERBOSE=ON publishToMavenLocal --info
popd
