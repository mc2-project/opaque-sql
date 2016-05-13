#!/bin/bash

set -eux

cd "$(dirname "$0")"

SGX_MODE=HW SGX_PRERELEASE=1 make

out=benchmark-results.txt

big=1048576
small=128
while [ $small -lt $big ]; do
    ./app $big $small | tee -a $out
    let small=$small*2
done
