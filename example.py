import sys
import bjsimple 

# ----------------------------------------------------------------------------
#
def resource_cb(origin, old_state, new_state):
    """CALLBACK FUNCTION: Writes BigJob state changes to STDERR.

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
    """CALLBACK FUNCTION: Writes Task state changes to STDERR
    """
    msg = " * Task %s state changed from '%s' to '%s'.\n" % \
        (str(origin), old_state, new_state)
    sys.stderr.write(msg)

    if new_state == bjsimple.FAILED:
        # Print the last log entry if task has failed to run
        print "   * LOG: %s" % origin.log[-1]

# ----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Start a new big job instance on stampede. All parameters a required,
    # except for 'project_id' which is optional. The meaning of the parameters
    # are as follows:
    #
    #    * name       - a name for easier identification 
    #    * resource   - the resource to use. The full resource dictionary 
    #                   is in 'bigjob_simple.py'. 
    #    * runtime    - runtime in minutes (a.k.a. wall-clock time)
    #    * cores      - total number of cores to allocate
    #    * workdir    - base working directory for all tasks
    #    * project_id - the project ID to use for billing
    #
    stampede = bjsimple.BigJobSimple(
        name       = "stampede:16cores", 
        resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], 
        runtime    = 1, 
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

    for i in range(0, 16):

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
                    "path"     : "/home1/00988/tg802352/loreipsum_pt2.txt"
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
