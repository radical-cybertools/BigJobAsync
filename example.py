#!/usr/bin/env python

"""This example illustrates how to submit N tasks to 
a remote resource, including input and output file transfer.

Inline comments explain specific components of the code. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import sys
import bjsimple 

# Number of tasks to run
N = 32

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

    if new_state == bjsimple.FAILED:
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

    if new_state == bjsimple.FAILED:
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
    stampede = bjsimple.Resource(
        name       = "stampede:16cores", 
        resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], 
        runtime    = 2, 
        cores      = 16, 
        workdir    = "/scratch/00988/tg802352/example/",
        project_id = "TG-MCB090174"
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

    for i in range(0, N):

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
        # Each input file transfer directive dictionary has the 
        # following structure:
        #    
        #    {
        #        "mode"     : bjsimple.COPY # currently the only 'mode'
        #        "path"     : path of the input file to copy 
        #        "location" : either bjsimple.LOCAL (on 'this' machine) or 
        #                     bjsimple.REMOTE (on the remote/executing machine)
        #    }
        #
        combinator_task = bjsimple.Task(
            name        = "combinator-task-%s" % i,
            cores       = 1,
            environment = {'OUTPUT_FILENAME': "loreipsum-%s.txt" % i},
            executable  = "/bin/bash",
            arguments   = ["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> $OUTPUT_FILENAME\""
            ], 
            input = [
                { 
                    # transfer an input file from the local machine (i.e., the machine
                    # where this script runs) into the task's workspace on the 
                    # remote machine.
                    "mode"        : bjsimple.COPY,
                    "origin"      : bjsimple.LOCAL,
                    "origin_path" : "/Users/oweidner/Work/Data/loreipsum_pt1.txt",
                },
                {
                    # copy an input file that is already in on the remote machine 
                    # into the task's workspace.
                    "mode"        : bjsimple.COPY, 
                    "origin"      : bjsimple.REMOTE, 
                    "origin_path" : "/home1/00988/tg802352/loreipsum_pt2.txt",
                }
            ], 
            output = [
                {
                    # transfer the task's output file ('STDOUT') back to the local machine 
                    # (i.e., the machine where this script runs).
                    "mode"             : bjsimple.COPY, 
                    "origin_path"      : "loreipsum-%s.txt" % i,      
                    "destination"      : bjsimple.LOCAL,
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

