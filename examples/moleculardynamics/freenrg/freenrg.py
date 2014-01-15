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
def run_test_task(resource_name, username, workdir, allocation):
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

    mmpbsa_test_task = bigjobasync.Task(
        name        = "MMPBSA-test-task",
        cores       = 1,
        #environment = {'OUTPUT_FILENAME': "loreipsum-complete-%s.txt" % i},
        executable  = "/bin/bash",
        arguments   = ["-l", "-c", "\"module load amber && echo -n MMPBSA path: && which MMPBSA.py && echo -n MMPBSA version: && MMPBSA.py --version\""],

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
if __name__ == "__main__":

    usage = "usage: %prog [--test]"
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('--test',
                      dest='test',
                      action="store_true",
                      help='Launch a test job to the resource defined in config.py.')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if options.test is True:
        run_test_task(CONFIG['resource'], CONFIG['username'], CONFIG['workdir'], CONFIG['allocation'])   
        sys.exit(0)
    else:
        print "not implemented yet"
        sys.exit(0)

