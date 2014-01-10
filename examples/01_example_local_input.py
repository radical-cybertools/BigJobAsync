#!/usr/bin/env python

"""This example illustrates how to submit N tasks to  a remote resource.

The scripts takes takes two input data files on the local machine
(loreipsum_pt1.txt, loreipsum_pt2.txt) and transfers it to the  remote machine
where they are assembled by a 'combinator' task. Once the task has finished,
the assembled output file is transferred back to te local machine.

Inline comments explain specific aspects of the code. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os, sys
import bigjobasync 

# ----------------------------------------------------------------------------
#
# Number of tasks to run
NUMTASKS    = 32
# CHANGE: Your stampede username
USERNAME    = "tg802352" 
# CHANGE: Your stampede working directory 
WORKDIR     = "/scratch/00988/tg802352/example/"
# CHANGE: Your stampede allocation
ALLOCATION  = "TG-MCB090174"

# ----------------------------------------------------------------------------
#
def resource_cb(origin, old_state, new_state):
    """Resource callback function: writes resource allocation state 
    changes to STDERR.

    It aborts the script script with exit code '-1' if the resource
    allocation state is 'FAILED'.

    (Obviously, more logic can be built into the callback function, for 
    example fault tolerance.)
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
if __name__ == "__main__":
    # Get a new resource allocation on stampede. All parameters a required,
    # except for 'project_id' which is optional. The meanings of the arguments
    # are as follows:
    #
    #    * name         a name for easier identification 
    #    * resource     the resource to use. The full resource dictionary 
    #                   is in 'resource_dictionary.py'. 
    #    * runtime      runtime in minutes (a.k.a. wall-clock time)
    #    * cores        total number of cores to allocate
    #    * workdir      base working directory for all tasks
    #    * project_id   the project ID to use for billing
    #
    stampede = bigjobasync.Resource(
        name       = "stampede", 
        resource   = bigjobasync.RESOURCES['XSEDE.STAMPEDE'],
        username   = USERNAME,
        runtime    = 5, 
        cores      = 16, 
        workdir    = WORKDIR,
        project_id = ALLOCATION
    )

    # Register a callback function with the resource allocation. This function 
    # will get called everytime the big job changes its state. Possible states 
    # of a resource allocation are: 
    #
    #    * NEW             (just created)
    #    * PENDING         (pilot waiting to get scheduled by the system)
    #    * RUNNING         (pilot executing on the resource)
    #    * DONE            (pilot successfully finished execution)
    #    * FAILED          (an error occured during pilot execution)
    #
    stampede.register_callbacks(resource_cb)
    stampede.allocate()

    # Define tasks and their input and output files
    all_tasks = []

    for i in range(0, NUMTASKS):

        # A 'combinator' tasks takes two input files and appends one to the 
        # other. The first input file 'loreipsum_pt1.txt' is copied from the
        # local machine to the executing cluster. The second file is already 
        # one the remote cluster and is copied locally into the task's
        # working directory. The resulting output file is copied back to the 
        # local machine. The meaning of the arguments are as follows: 
        #
        #    * name          a name for easier identification 
        #    * cores         the number of cores required by this task 
        #                    (the default is 1)
        #    * environment   a dictionary of environment variables to set 
        #                    in the task's executable environment 
        #    * executable    the executable represented by the task
        #    * arguments     a list of arguments passed to the executable
        #    * input         a list of input file transfer directives (dicts)
        #    * output        a list of output file transfer directives (dicts)
        # 
        combinator_task = bigjobasync.Task(
            name        = "combinator-task-%s" % i,
            cores       = 1,
            environment = {'OUTPUT_FILENAME': "loreipsum-complete-%s.txt" % i},
            executable  = "/bin/bash",
            arguments   = ["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> $OUTPUT_FILENAME\""
            ],
            # transfer input files from the local machine (i.e., the machine
            # where this script runs) into the task's workspace on the 
            # remote machine. 
            input = [
                { 
                    "mode"        : bigjobasync.COPY,
                    "origin"      : bigjobasync.LOCAL,
                    "origin_path" : "/%s/loreipsum_pt1.txt" % os.getcwd(),
                },
                {
                    "mode"        : bigjobasync.COPY, 
                    "origin"      : bigjobasync.LOCAL, 
                    "origin_path" : "/%s/loreipsum_pt2.txt" % os.getcwd(),
                }
            ], 
            output = [
                {
                    # transfer the task's output file ('STDOUT') back to the local machine 
                    # (i.e., the machine where this script runs).
                    "mode"             : bigjobasync.COPY, 
                    "origin_path"      : "loreipsum-complete-%s.txt" % i,      
                    "destination"      : bigjobasync.LOCAL,
                    "destination_path" : "."
                }
            ]
        )

        # Register a callback function with each task. This function will get 
        # called everytime the task changes its state. Possible states of a 
        # task are: 
        #
        #    * NEW                   (task just created)
        #    * TRANSFERRING_INPUT    (task transferring input data)
        #    * WAITING_FOR_EXECUTION (task waiting to get submitted)
        #    * PENDING               (task submitted, waiting to get executed)
        #    * RUNNING               (task executing on the resource)
        #    * TRANSFERRING_OUTPUT   (task transferring output data)
        #    * DONE                  (task successfully finished execution)
        #    * FAILED                (error during transfer or execution)
        #
        combinator_task.register_callbacks(task_cb)
        all_tasks.append(combinator_task)

    # Submit all tasks to stampede
    stampede.schedule_tasks(all_tasks)
    
    # Wait for the Resource allocation to finish, i.e., run out of wall time
    stampede.wait()

    sys.exit(0)

