#!/bin/bash

rm -r TileDB-Java
git clone https://github.com/TileDB-Inc/TileDB-Java.git
cd TileDB-Java
git checkout npapa/fixVersion
git pull
gradle assemble publishToMavenLocal
