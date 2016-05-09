#!/bin/bash

set -eu

sudo yum --enablerepo epel install s3cmd   # on Amazon Linux
s3cmd --configure
mkdir -p /mnt/datasets/big-data-benchmark-files/{rankings,uservisits}/{tiny,1node}

s3cmd sync s3://big-data-benchmark/pavlo/text-deflate/tiny/rankings/ /mnt/datasets/big-data-benchmark-files/rankings/tiny/
s3cmd sync s3://big-data-benchmark/pavlo/text-deflate/tiny/uservisits/ /mnt/datasets/big-data-benchmark-files/uservisits/tiny/
s3cmd sync s3://big-data-benchmark/pavlo/text-deflate/1node/rankings/ /mnt/datasets/big-data-benchmark-files/rankings/1node/
s3cmd sync s3://big-data-benchmark/pavlo/text-deflate/1node/uservisits/ /mnt/datasets/big-data-benchmark-files/uservisits/1node/

for dir in /mnt/datasets/big-data-benchmark-files/{rankings,uservisits}/{tiny,1node}; do
    echo $dir
    pushd $dir
    for file in *.deflate; do echo $file; python -c 'import zlib,sys;print zlib.decompress(sys.stdin.read())' < $file > ${file%.deflate}.txt; rm $file; done
    popd
done

mkdir -p /mnt/datasets/big-data-benchmark-files/rankings/1million
cp /mnt/datasets/big-data-benchmark-files/rankings/1node/00000{0..2}_0.txt /mnt/datasets/big-data-benchmark-files/rankings/1million/
mkdir -p /mnt/datasets/big-data-benchmark-files/uservisits/1million
cp /mnt/datasets/big-data-benchmark-files/uservisits/1node/00000{0..2}_0.txt /mnt/datasets/big-data-benchmark-files/uservisits/1million/
