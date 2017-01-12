#!/bin/bash

set -eux

mkdir -p $DATA_DIR/bdb/rankings/1million
s3cmd sync s3://ankurdave/bdb-rankings-1million/ $DATA_DIR/bdb/rankings/1million/
mkdir -p $DATA_DIR/bdb/uservisits/1million
s3cmd sync s3://ankurdave/bdb-uservisits-1million/ $DATA_DIR/bdb/uservisits/1million/
mkdir -p $DATA_DIR/pagerank
s3cmd sync s3://ankurdave/opaque-pagerank/ $DATA_DIR/pagerank/
