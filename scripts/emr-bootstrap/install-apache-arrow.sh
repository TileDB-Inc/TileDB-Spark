#!/bin/bash

sudo yum install -y gcc g++ cmake3 git flex boost boost-thread boost-devel \
    boost-regex boost-system boost-filesystem \
    gtk-doc glib-devel pkgconfig \
    autoconf automake binutils \
    bison flex gcc gcc-c++ gettext \
    libtool make patch pkgconfig \
    ctags elfutils indent patchutils glib2-devel gobject-introspection-devel

pip-3.6 install --user meson

sudo yum groupinstall -y "Development tools"
git clone https://github.com/apache/arrow.git -b apache-arrow-0.14.1 &&
cd arrow/cpp &&
mkdir build &&
cd build &&
cmake3 .. -DCMAKE_INSTALL_PREFIX=/usr/ -DARROW_PARQUET=ON &&
make -j$(nproc) &&
sudo make install &&
cd ../ && rm -r build
