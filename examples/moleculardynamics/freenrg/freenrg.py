#!/usr/bin/env python

"""This example illustrates how to run free energy calculations with Amber
MMPBSA.py for N replicas. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os, sys, uuid
import optparse
import bigjobasync 

from config import CONFIG
from kernel import KERNEL

# ----------------------------------------------------------------------------
#
def resource_cb(origin, old_state, new_state):
    """Resource callback function: writes resource allocation state 
    changes to STDERR.
    """ 
    msg = " * Resource '%s' state changed from '%s' to '%s'.\n" % \
        (str(origin), old_state, new_state)
    sys.stderr.write(msg)

    if new_state == bigjobasync.FAILED:
        # Print the log and exit if big job has failed
        for entry in origin.log:
            print "   * LOG: %s" % entry
        sys.stderr.write("   * EXITING.\n")
        sys.exit(-1)

# ----------------------------------------------------------------------------
#
def task_cb(origin, old_state, new_state):
    """Task callback function: writes task state changes to STDERR
    """
    msg = " * Task %s state changed from '%s' to '%s'.\n" % \
        (str(origin), old_state, new_state)
    sys.stderr.write(msg)

    if new_state == bigjobasync.FAILED:
        # Print the log entry if task has failed to run
        for entry in origin.log:
            print "     LOG: %s" % entry

# ----------------------------------------------------------------------------
#
def run_test_job(resource_name, username, workdir, allocation):
    """Runs a single FE test job.
    """

    ############################################################
    # The resource allocation
    cluster = bigjobasync.Resource(
        name       = resource_name, 
        resource   = bigjobasync.RESOURCES[resource_name],
        username   = username,
        runtime    = 5, 
        cores      = 16, 
        workdir    = workdir,
        project_id = allocation
    )
    cluster.register_callbacks(resource_cb)
    cluster.allocate(terminate_on_empty_queue=True)

    ############################################################
    # The test task
    output_file = "/tmp/MMPBSA-test-task-%s" % str(uuid.uuid4())

    kernelcfg = KERNEL["MMPBSA"]["resources"][resource_name]

    mmpbsa_test_task = bigjobasync.Task(
        name        = "MMPBSA-fe-test-task",
        cores       = 1,
        environment = kernelcfg["environment"],
        executable  = "/bin/bash",
        arguments   = ["-l", "-c", "\"%s && %s -i ~/MMPBSASampleDATA/nmode.py -cp ~/MMPBSASampleDATA/com.top.2 -rp ~/MMPBSASampleDATA/rec.top.2 -lp ~/MMPBSASampleDATA/lig.top -y ~/MMPBSASampleDATA/rep10.traj \"" % \
            (kernelcfg["pre_execution"], kernelcfg["executable"], kernelcfg["executable"]) ],

        output = [
            {
                "mode"             : bigjobasync.COPY, 
                "origin_path"      : "STDOUT" ,      
                "destination"      : bigjobasync.LOCAL,
                "destination_path" : output_file
            }
        ]
    )
    mmpbsa_test_task.register_callbacks(task_cb)

    cluster.schedule_tasks([mmpbsa_test_task])
    cluster.wait()

    if mmpbsa_test_task.state is bigjobasync.FAILED:
        print "\nERROR: Couldn't run test task."
    else:
        print "\nTest task results:"
        with open(output_file, 'r') as content_file:
            content = content_file.read()
            print content
        # remove output file
        os.remove(output_file)


# ----------------------------------------------------------------------------
#
def run_sanity_check(resource_name, username, workdir, allocation):
    """Runs a simple job that performs some sanity tests, determines 
    AMBER version, etc.
    """

    ############################################################
    # The resource allocation
    cluster = bigjobasync.Resource(
        name       = resource_name, 
        resource   = bigjobasync.RESOURCES[resource_name],
        username   = username,
        runtime    = 5, 
        cores      = 16, 
        workdir    = workdir,
        project_id = allocation
    )
    cluster.register_callbacks(resource_cb)
    cluster.allocate(terminate_on_empty_queue=True)

    ############################################################
    # The test task
    output_file = "/tmp/MMPBSA-test-task-%s" % str(uuid.uuid4())

    kernelcfg = KERNEL["MMPBSA"]["resources"][resource_name]

    mmpbsa_check_task = bigjobasync.Task(
        name        = "MMPBSA-check-task",
        cores       = 1,
        environment = kernelcfg["environment"],
        executable  = "/bin/bash",
        arguments   = ["-l", "-c", "\"%s && echo -n MMPBSA path: && which %s && echo -n MMPBSA version: && %s --version\"" % \
            (kernelcfg["pre_execution"], kernelcfg["executable"], kernelcfg["executable"]) ],

        output = [
            {
                "mode"             : bigjobasync.COPY, 
                "origin_path"      : "STDOUT" ,      
                "destination"      : bigjobasync.LOCAL,
                "destination_path" : output_file
            }
        ]
    )
    mmpbsa_check_task.register_callbacks(task_cb)

    cluster.schedule_tasks([mmpbsa_check_task])
    cluster.wait()

    if mmpbsa_test_task.state is bigjobasync.FAILED:
        print "\nERROR: Couldn't run check task."
    else:
        print "\Check task results:"
        with open(output_file, 'r') as content_file:
            content = content_file.read()
            print content
        # remove output file
        os.remove(output_file)

# ----------------------------------------------------------------------------
#
if __name__ == "__main__":

    usage = "usage: %prog [--checkenv, --testjob]"
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('--checkenv',
                      dest='checkenv',
                      action="store_true",
                      help='Launch a test job to check the execution evnironment.')

    parser.add_option('--testjob',
                      dest='testjob',
                      action="store_true",
                      help='Launch a test job with a single FE calculation.')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if options.checkenv is True:
        run_sanity_check(CONFIG['resource'], CONFIG['username'], CONFIG['workdir'], CONFIG['allocation'])   
        sys.exit(0)

    elif options.testjob is True:
        run_test_job(CONFIG['resource'], CONFIG['username'], CONFIG['workdir'], CONFIG['allocation'])   
        sys.exit(0)

    else:
        print "not implemented yet"
        sys.exit(0)

