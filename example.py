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
        # Print the log and exit if bigjob has failed to run
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
        # Print the log if task has failed to run
        for entry in origin.log:
            print "   * LOG: %s" % entry

# ----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # start a new big job instance on stampede
    stampede = bjsimple.BigJobSimple(
        name       = "stampede:16cores", # give the bigjob instance a name
        resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], # resource
        runtime    = 1, # bigjob runtime in minutes (a.k.a. wall-clock time)
        cores      = 16, # total number of cores to allocate
        workdir    = "/scratch/00988/tg802352/example/", # working directory
        project_id = "TG-MCB090174", # project ID to use for billing
    )

    stampede.register_callbacks(resource_cb)
    stampede.allocate()

    # define tasks and their input and output files
    all_tasks = []

    for i in range(0, 16):

        # A 'combinator' tasks takes two input files and appends one to the 
        # other. The first input file 'loreipsum_pt1.txt' is copied from the
        # local machine to the executing cluster. The second file is already 
        # one the remote cluster and is copied locally into the task's
        # working directory. The resulting output file 'loreipsum.txt' is 
        # copied back to the local machine.
        combinator = bjsimple.Task(
            name       = "my-task-%s" % i,
            executable = "/bin/bash",
            arguments  = ["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> loreipsum.txt\""
            ], 
            input = [
                {   # RENAME origin -> location
                    "location" : bjsimple.LOCAL,  "mode": bjsimple.COPY, 
                    "path"     : "/Users/oweidner/Work/Data/loreipsum_pt1.txt"
                },
                {
                    "location" : bjsimple.REMOTE, "mode": bjsimple.COPY, 
                    "path"     : "/home1/00988/tg802352/loreipsum_pt2.txt"
                }
            ], 
            output = [
                {
                    "path"        : "loreipsum.txt", 
                    "destination" : "/tmp/loreipsum-%s.txt" % i
                }
            ]
        )
        combinator.register_callbacks(task_cb)
        all_tasks.append(combinator)

    # submit them to stampede
    stampede.schedule_tasks(all_tasks)
    
    # wait for everything to finish
    stampede.wait()
