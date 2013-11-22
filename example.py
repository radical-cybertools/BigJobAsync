import sys
import bjsimple 

# ----------------------------------------------------------------------------
#
def resource_cb(origin, old_state, new_state):
    """CALLBACK FUNCTION: Writes BigJob state changes to STDERR.

    It aborts the script script with exit code '0' if BigJob state is 'DONE' 
    or with exit code '-1' if BigJob state is 'FAILED'.

    Obviously, more logic can be built into the callback function, for 
    example fault tolerance.
    """ 
    msg = " * BigJob %s state changed from '%s' to '%s'.\n" % \
        (origin, old_state, new_state)
    sys.stderr.write(msg)

    if new_state == bjsimple.FAILED:
        # Print the log and exit if bigjob has failed to run
        for entry in origin.log:
            print "   * LOG: %s" % entry
        sys.stderr.write("   * EXITING.\n")
        sys.exit(-1)

    elif new_state == bjsimple.DONE:
        # Exit if bigjob is done running 
        sys.stderr.write("   ... EXITING.\n")
        sys.exit(0)

# ----------------------------------------------------------------------------
#
def task_cb(origin, old_state, new_state):
    """CALLBACK FUNCTION: Writes Task state changes to STDERR
    """
    msg = " * Task %s has changed from '%s' to '%s'.\n".format(
        origin, old_state, new_state)
    sys.stderr.write(msg)

# ----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # start a new big job instance on stampede
    stampede = bjsimple.BigJobSimple(
        resource=bjsimple.RESOURCES['XSEDE.STAMPEDE'],
        runtime=5, # minutes
        cores=64,
        project_id="TG-MCB090174",
        base_dir="/home1/00988/tg802352/")

    stampede.register_callbacks(resource_cb)
    stampede.allocate()

    # define the tasks and their data 
    my_tasks = []

    for i in range(0, 128):
        task = bjsimple.Task(
            name="my-task-%s".format(i),
            executable="/bin/cat",
            arguments=["SDSD"], 
            transfer_input=[], 
            transfer_output=[])
        task.register_callbacks(task_cb)
        my_tasks.append(task)

    # submit them to stampede
    stampede.schedule_tasks(my_tasks)
    
    # wait for everything to finish
    stampede.wait()
