#!/bin/bash

set -eux

mkdir -p $DATA_DIR/big-data-benchmark-files/rankings/tiny
s3cmd sync s3://big-data-benchmark/pavlo/text/tiny/rankings/ $DATA_DIR/big-data-benchmark-files/rankings/tiny/
mkdir -p $DATA_DIR/big-data-benchmark-files/uservisits/tiny
s3cmd sync s3://big-data-benchmark/pavlo/text/tiny/uservisits/ $DATA_DIR/big-data-benchmark-files/uservisits/tiny/
mkdir -p $DATA_DIR/big-data-benchmark-files/rankings/1million
s3cmd sync s3://ankurdave/bdb-rankings-1million/ $DATA_DIR/big-data-benchmark-files/rankings/1million/
mkdir -p $DATA_DIR/big-data-benchmark-files/uservisits/1million
s3cmd sync s3://ankurdave/bdb-uservisits-1million/ $DATA_DIR/big-data-benchmark-files/uservisits/1million/
mkdir -p $DATA_DIR/pagerank-files
s3cmd sync s3://ankurdave/opaque-pagerank/ $DATA_DIR/pagerank-files/

$REPO_DIR/data/opaque/disease/synth-all-data

git clone https://github.com/electrum/tpch-dbgen
pushd tpch-dbgen
make
./dbgen -vf -s 0.01
mkdir -p $DATA_DIR/tpch/sf_small
cp *.tbl $DATA_DIR/tpch/sf_small/
./dbgen -vf -s 1
mkdir -p $DATA_DIR/tpch/sf1
cp *.tbl $DATA_DIR/tpch/sf1/
./dbgen -vf -s 0.2
mkdir -p $DATA_DIR/tpch/sf0.2
cp *.tbl $DATA_DIR/tpch/sf0.2/
popd
