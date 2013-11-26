#!/usr/bin/env python

"""This example shows how to scale a set of tasks across two 
different resources. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import sys
import bjsimple 

# ----------------------------------------------------------------------------
#
def resource_cb(origin, old_state, new_state):
    """Big job callback function: writes BigJob state changes to STDERR.

    It aborts the script script with exit code '-1' if BigJob 
    state is 'FAILED'.

    Obviously, more logic can be built into the callback function, for 
    example fault tolerance.
    """ 
    msg = " * BigJob '%s' state changed from '%s' to '%s'.\n" % \
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
    """The main function.
    """

    alamo = bjsimple.BigJobSimple(
        name       = "alamo:16cores", 
        resource   = bjsimple.RESOURCES['FUTUREGRID.ALAMO'], 
        runtime    = 20, 
        cores      = 16, 
        workdir    = "/N/work/oweidner/example/",
    )
    alamo.register_callbacks(resource_cb)

    india = bjsimple.BigJobSimple(
        name       = "india:16cores", 
        resource   = bjsimple.RESOURCES['FUTUREGRID.INDIA'], 
        runtime    = 20, 
        cores      = 16, 
        workdir    = "/N/u/oweidner/example/",
    )
    india.register_callbacks(resource_cb)

    india.allocate()
    alamo.allocate()

    # Define tasks and their input and output files
    all_tasks = []

    for i in range(0, 32):

        # A 'combinator' tasks takes two input files and appends one to the 
        # other. The first input file 'loreipsum_pt1.txt' is copied from the
        # local machine to the executing cluster. The second file is already 
        # one the remote cluster and is copied locally into the task's
        # working directory. The resulting output file 'loreipsum.txt' is 
        # copied back to the local machine.
        combinator_task = bjsimple.Task(
            name       = "my-task-%s" % i,
            executable = "/bin/bash",
            arguments  = ["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> loreipsum.txt\""
            ], 
            input = [
                { 
                    "location" : bjsimple.LOCAL, # file is on local machine 
                    "mode"     : bjsimple.COPY,  # copy it 
                    "path"     : "/Users/oweidner/Work/Data/loreipsum_pt1.txt"
                },
                {
                    "location" : bjsimple.REMOTE, # file is already on the remote machine 
                    "mode"     : bjsimple.COPY,   # ('LINK' will be a future option) 
                    "path"     : "/N/u/oweidner/loreipsum_pt2.txt"
                }
            ], 
            output = [
                {
                    # TODO -- doesn't work yet, i.e., output doesn't get copied back
                    "path"        : "loreipsum.txt", 
                    "destination" : "/tmp/loreipsum-%s.txt" % i
                }
            ]
        )
        combinator_task.register_callbacks(task_cb)
        all_tasks.append(combinator_task)

    # Do a simple round-robin task distribution
    targets = [india, alamo]
    next = 0

    for task in all_tasks:
        print "Scheduling %s on %s" % (str(task), str(targets[next]))
        targets[next].schedule_tasks(task)
        if next < len(targets)-1:
            next += 1
        else:
            next = 0
    
    # Wait for both BigJobs to finish
    india.wait()
    alamo.wait()

    sys.exit(0)
