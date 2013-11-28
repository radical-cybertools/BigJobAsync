#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import sys
import time
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

    # Start a new big job instance on stampede. All parameters a required,
    # except for 'project_id' which is optional. The meanings of the arguments
    # are as follows:
    #
    #    * name       - a name for easier identification 
    #    * resource   - the resource to use. The full resource dictionary 
    #                   is in 'resource_dictionary.py'. 
    #    * runtime    - runtime in minutes (a.k.a. wall-clock time)
    #    * cores      - total number of cores to allocate
    #    * workdir    - base working directory for all tasks
    #    * project_id - the project ID to use for billing
    #
    stampede = bjsimple.Resource(
        name       = "stampede:16cores", 
        resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], 
        runtime    = 2, 
        cores      = 16, 
        workdir    = "/scratch/00988/tg802352/example/",
        project_id = "TG-MCB090174"
    )

    # Register a callback function with the big job. This function will get 
    # called everytime the big job changes its state. Possible states of a 
    # big job are: 
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

    for i in range(0, 20):

        # A 'combinator' tasks takes two input files and appends one to the 
        # other. The first input file 'loreipsum_pt1.txt' is copied from the
        # local machine to the executing cluster. The second file is already 
        # one the remote cluster and is copied locally into the task's
        # working directory. The resulting output file is copied back to the 
        # local machine. The meaning of the arguments are as follows: 
        #
        #    * name        - a name for easier identification 
        #    * cores       - the number of cores required by this task 
        #                    (the default is 1)
        #    * environment - a dictionary of environment variables to set 
        #                    in the task's executable environment 
        #    * executable  - the executable represented by the task
        #    * arguments   - a list of arguments passed to the executable
        #    * input       - a list of input file transfer directives (dicts)
        #    * output      - a list of output file transfer directives (dicts)
        # 
        # Each input file transfer directive has the following structure:
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
                    "mode"     : bjsimple.COPY,  # copy it 
                    "path"     : "/Users/oweidner/Work/Data/loreipsum_pt1.txt",
                    "location" : bjsimple.LOCAL, # file is on local machine 

                },
                {
                    "mode"     : bjsimple.COPY,   # ('LINK' will be a future option) 
                    "path"     : "/home1/00988/tg802352/loreipsum_pt2.txt",
                    "location" : bjsimple.REMOTE, # file is already on the remote machine 
                }
            ], 
            # output = [
            #     {
            #         # TODO -- doesn't work yet, i.e., output doesn't get copied back
            #         "path"        : "loreipsum-%s.txt" % i, 
            #         "destination" : "." % i
            #     }
            # ]
        )

        # Register a callback function with each task. This function will get 
        # called everytime the task changes its state. Possible states of a 
        # task are: 
        #
        #    * NEW             (task just created)
        #    * PENDING         (task waiting to get scheduled)
        #    * TRANSFER_INPUT  (task transferring input data)
        #    * RUNNING         (task executing on the resource)
        #    * TRANSFER_OUTPUT (task transferring output data)
        #    * DONE            (task successfully finished execution)
        #    * FAILED          (an error occured during transfer or execution)
        #
        combinator_task.register_callbacks(task_cb)
        all_tasks.append(combinator_task)

    # Submit all tasks to stampede
    stampede.schedule_tasks(all_tasks)
    
    # Wait for the BigJob to finish
    stampede.wait()

    sys.exit(0)

