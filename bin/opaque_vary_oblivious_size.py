"""
Run an experiment varying the size of the oblivious region.

Run this with:

    python -u bin/opaque_vary_oblivious_size.py 2>&1 | tee ~/vary-oblivious.txt

"""
import subprocess
import os

def main():
    os.environ['SGX_PERF'] = '1'
    for hw_mode in [True, False]:
        os.environ['SGX_MODE'] = 'HW' if hw_mode else ''
        if not hw_mode:
            subprocess.check_call(
                'sed -i "s/SGX_MODE=HW //" sql/enclave/build.sh',
                shell=True)
        for sort_buffer_mb in range(4, 128, 4):
            print '=== Running sort buffer size: %d' % (sort_buffer_mb, )
            subprocess.check_call(
                ('sed -i "s/^#define MAX_SORT_BUFFER .*/'
                 '#define MAX_SORT_BUFFER (%d * 1024 * 1024)/" sql/enclave/Include/define.h') %
                (sort_buffer_mb, ),
                shell=True)
            subprocess.check_call(
                './build/sbt assembly',
                shell=True)
            subprocess.check_call(
                'bin/spark-submit --master local[1] --conf spark.ui.showConsoleProgress=false '
                '--class org.apache.spark.sql.QEDBenchmark '
                'assembly/target/scala-2.11/spark-assembly-2.0.0-SNAPSHOT-hadoop2.2.0.jar',
                shell=True)
            print '=== Sort buffer size: %d' % (sort_buffer_mb, )

if __name__ == '__main__':
    main()
