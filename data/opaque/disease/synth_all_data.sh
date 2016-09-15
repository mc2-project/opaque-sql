#!/bin/bash

set -eu

cd "$(dirname $0)"

source ../../../conf/spark-env.sh

mkdir -p $SPARKSGX_DATA_DIR/disease

python parse_health_codes.py
sed 's/^\([^,]\+\),/\1|/' icd_codes.csv > icd_codes.tsv
mv icd_codes.tsv $SPARKSGX_DATA_DIR/disease/

python synth_treatment_data.py
mv treatment.csv $SPARKSGX_DATA_DIR/disease/

python synth_patient_data.py 125
python synth_patient_data.py 250
python synth_patient_data.py 500
python synth_patient_data.py 1000
python synth_patient_data.py 2000
python synth_patient_data.py 4000
python synth_patient_data.py 8000
python synth_patient_data.py 16000
python synth_patient_data.py 32000
python synth_patient_data.py 64000
python synth_patient_data.py 128000
python synth_patient_data.py 256000
python synth_patient_data.py 512000
python synth_patient_data.py 1024000
mv patient-*.csv $SPARKSGX_DATA_DIR/disease/
