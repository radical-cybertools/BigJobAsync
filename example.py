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
        name="ALAMO:8cores",
        resource=bjsimple.RESOURCES['FUTUREGRID.ALAMO'],
        runtime=5, # minutes
        cores=8,
        workdir="/N/work/oweidner/example/"
    )

    stampede.register_callbacks(resource_cb)
    stampede.allocate()

    # define tasks and their input and output files
    my_tasks = []

    for i in range(0, 2):
        task = bjsimple.Task(
            name="my-task-%s" % i,
            executable="/bin/bash",
            arguments=["-c", "\"/bin/cat loreipsum_pt1.txt loreipsum_pt2.txt >> loreipsum.txt\""
            ], 
            input=[
                {   # RENAME origin -> location
                    "location" : bjsimple.LOCAL,  "mode": bjsimple.COPY, 
                    "path"     : "/Users/oweidner/Work/Data/loreipsum_pt1.txt"
                },
                {
                    "location" : bjsimple.REMOTE, "mode": bjsimple.COPY, 
                    "path"     : "/N/u/oweidner/loreipsum_pt2.txt"
                }
            ], 
            output=[
                {
                    "path" : "loreipsum.txt", "destination" : "."
                }
            ]
        )
        task.register_callbacks(task_cb)
        my_tasks.append(task)

    # submit them to stampede
    stampede.schedule_tasks(my_tasks)
    
    # wait for everything to finish
    stampede.wait()
