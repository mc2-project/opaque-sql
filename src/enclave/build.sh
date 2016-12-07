#!/bin/bash

set -eu

cd "$( dirname "${BASH_SOURCE[0]}" )"

#make SGX_MODE=HW SGX_DEBUG=1
make SGX_MODE=HW SGX_PRERELEASE=1
#SGX_DEBUG=1
