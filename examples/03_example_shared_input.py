#!/usr/bin/env python

"""DOCSTRING

Inline comments explain specific components of the code. 
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
task_counter = 0
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

    stampede = bigjobasync.Resource(
        name       = "stampede", 
        resource   = bigjobasync.RESOURCES['XSEDE.STAMPEDE'],
        username   = USERNAME,
        runtime    = 5, 
        cores      = 16, 
        workdir    = WORKDIR,
        project_id = ALLOCATION
    )

    stampede.register_callbacks(resource_cb)
    # If terminate_on_empty_queue=True, the resource will be shut down as soon
    # as the last task has finished. 
    stampede.allocate(terminate_on_empty_queue=True)

    # Define tasks and their input and output files
    all_tasks = []

    # The first task is a 'dummy' task that just transfers the 
    # input file that is shared between all other tasks
    data_staging_task = bigjobasync.Task(
        name          = "data-staging-task",
        cores         = 1,
        executable    = "/bin/true",
        input = [
            {
                "mode"        : bigjobasync.COPY,
                "origin"      : bigjobasync.LOCAL,
                "origin_path" : "/%s/loreipsum_pt1.txt" % os.getcwd(),
            }
        ] 
    )
    data_staging_task.register_callbacks(task_cb)
    all_tasks.append(data_staging_task)

    # Now we define the compute tasks that use the shared input data.
    for i in range(0, NUMTASKS):

        combinator_task = bigjobasync.Task(
            name        = "combinator-task-%s" % i,
            cores       = 1,
            executable  = "/bin/bash",
            arguments   = ["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> STDOUT\""], 
            input = [
                {
                    "mode"        : bigjobasync.LINK,  # create a symbolic link to the shared input file
                    "origin"      : data_staging_task, # file is 'part' of a different task 
                    "origin_path" : "loreipsum_pt1.txt"
                },
                {
                    "mode"        : bigjobasync.COPY,    
                    "origin"      : bigjobasync.REMOTE,  
                    "origin_path" : "/home1/00988/tg802352/loreipsum_pt2.txt",
                }
            ], 
            output = [
                {
                    "mode"             : bigjobasync.COPY, 
                    "origin_path"      : "STDOUT",         
                    "destination"      : bigjobasync.LOCAL,        
                    "destination_path" : "STDOUT-from-task-%s" % i 
                }
            ]
        )

        combinator_task.register_callbacks(task_cb)
        all_tasks.append(combinator_task)

    # Submit all tasks to stampede
    stampede.schedule_tasks(all_tasks)
    
    # Wait for the Resource allocation to finish, i.e., run out of wall time
    # If terminate_on_empty_queue=True, the resource will be shut down as soon
    # as the last task has finished. 
    stampede.wait()

    sys.exit(0)

