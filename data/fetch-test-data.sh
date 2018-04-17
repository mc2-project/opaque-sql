#!/bin/sh
git clone https://github.com/electrum/tpch-dbgen
cd tpch-dbgen
make
./dbgen -vf -s 0.01
mkdir -p $SPARKSGX_DATA_DIR/tpch/sf_small
cp *.tbl $SPARKSGX_DATA_DIR/tpch/sf_small/
