#!/usr/bin/python
# _*_ coding:UTF-8

import os
import subprocess
import sys
import signal

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print "Usage: polardb_cluster_manager_control.py [work_dir] [start|stop|status]"
        sys.exit(0)


    exit_code = 0
    work_dir = sys.argv[1]
    supervisor_pid_file = work_dir + "/supervisor_pid"
    cm_pid_file = work_dir + "/cm_pid"
    conf_file = work_dir + "/conf/polardb_cluster_manager.conf"
    run_args = ""
    if len(sys.argv) > 3:
        run_args =  " ".join(sys.argv[3:])

    if not os.path.exists(conf_file):
        print "conf file ", conf_file, " in ", work_dir, " not exist"
        sys.exit(-1)

    if sys.argv[2] == "start":
        try:
            f = open(supervisor_pid_file)
            print "PolarDB ClusterManager Supervisor PidFile", supervisor_pid_file, " Already Exist, Please check!"
            sys.exit(0)
        except Exception as e:
            pass
        p = subprocess.Popen("/usr/local/polardb_cluster_manager/bin/supervisor.py " + work_dir
                             + " /usr/local/polardb_cluster_manager/bin/polardb-cluster-manager"
                             + " --work_dir=" + work_dir + " " + run_args, shell=True)
        f = open(supervisor_pid_file, "w")
        f.write(str(p.pid))
        f.close()
    elif sys.argv[2] == "stop":
        try:
            f = open(supervisor_pid_file)
            supervisor_pid = f.read()
            f.close()
            os.remove(supervisor_pid_file)
        except Exception as e:
            print "PolarDB ClusterManager Supervisor PidFile", supervisor_pid_file, "Not Exist, Please check!"
            sys.exit(0)

        try:
            os.kill(int(supervisor_pid), signal.SIGKILL)
        except Exception as e:
            print "PolarDB ClusterManager Supervisor Pid", supervisor_pid, "Not Exist, Please check!"

        try:
            f = open(cm_pid_file)
            cm_pid = f.read()
            f.close()
        except Exception as e:
            print "PolarDB ClusterManager PidFile", cm_pid_file, "Not Exist, Please check!"
            sys.exit(0)

        try:
            os.kill(int(cm_pid), signal.SIGKILL)
        except Exception as e:
            print "PolarDB ClusterManager Pid", cm_pid, "Not Exist, Please check!"
    elif sys.argv[2] == "status":
        try:
            f = open(cm_pid_file)
            cm_pid = f.read()
            f.close()
        except Exception as e:
            print "PolarDB ClusterManager PidFile", cm_pid_file, "Not Exist, Please check!"
            sys.exit(0)

        try:
            os.kill(int(cm_pid), 0)
        except Exception as e:
            print "PolarDB ClusterManager Work On", sys.argv[1], "IS NOT RUNNING"
            sys.exit(0)
        print "PolarDB ClusterManager Work ON", sys.argv[1], "IS RUNNING"
    else:
        print "Usage: polardb_cluster_manager_control.py [work_dir] [start|stop|status]"
        sys.exit(0)
