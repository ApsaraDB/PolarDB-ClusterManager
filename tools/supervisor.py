#!/usr/bin/python
# _*_ coding:UTF-8

import subprocess
import sys
import time
import logging

def run_supervisor():
    failover_sleep_time = 5
    log_path = sys.argv[1] + "/supervisor.log"
    pid_file = sys.argv[1] + "/cm_pid"

    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"

    logging.basicConfig(filename=log_path, level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

    while True:
        cmd = " ".join(sys.argv[2:])
        logging.info("Start the PolarDB ClusterManager with cmd %s", cmd)
        p = subprocess.Popen(cmd, shell=True)
        f = open(pid_file, "w")
        f.write(str(p.pid))
        f.close()

        out, err = p.communicate()
        logging.info("PolarDB ClusterManager exit with code %d, out: %s stderr: %s", p.returncode, out, err)

        time.sleep(failover_sleep_time)



if __name__ == '__main__':
    print sys.argv
    try:
        run_supervisor()
    except Exception as e:
        logging.warning(e)
