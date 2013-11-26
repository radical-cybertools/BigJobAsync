#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

# global imports 
import saga

# local imports
from constants import * 


# ----------------------------------------------------------------------------
# 
def _output_file_transfer_worker(remote_work_dir_url, task):
    """DOCSTRING
    """
    task._set_and_propagate_state_change_priv(new_state=TRANSFER_OUTPUT)

    print "trasnferring...."

    task._set_and_propagate_state_change_priv(new_state=DONE)

# ----------------------------------------------------------------------------
# 
def _file_transfer_worker(remote_work_dir_url, task):
    """DOCSTRING
    """
    # Iterate over the tasks and try to submit them to BigJob after the 
    # input data has been staged. 
    try:
        # create working directories for tasks based on the task uid
        task_workdir_url = "%s/%s" % (remote_work_dir_url, task.dir_name)
        task._log.append("Creating working directory '%s'" % task.dir_name)

        task_workdir = saga.filesystem.Directory(task_workdir_url, 
            saga.filesystem.CREATE_PARENTS)
    except Exception, ex:
        task._log.append(str(ex))
        task._set_and_propagate_state_change_priv(new_state=FAILED)
        return task

    # Next we can take care of the file transfers
    # Change the state to 'TransferInput'
    task._set_and_propagate_state_change_priv(new_state=TRANSFER_INPUT)

    for directive in task._input:

        if directive['location'] == LOCAL:
            try: 
                # we use saga-python to copy a local file to the 
                # remote destination
                task._log.append("Copying LOCAL input file '%s'" % directive['path'])
                local_filename = "file://localhost//%s" % directive['path']
                local_file = saga.filesystem.File(local_filename)
                local_file.copy(task_workdir_url)
                local_file.close()
            except Exception, ex:
                task._log.append(str(ex))
                task._set_and_propagate_state_change_priv(new_state=FAILED)
                return task

        elif directive['location'] == REMOTE:
            try: 
                # copy around stuff locally on the remote machine
                task._log.append("Copying REMOTE input file '%s'" % directive['path'])
                task_workdir.copy(directive['path'], ".")
            except Exception, ex:
                task._log.append(str(ex))
                task._set_and_propagate_state_change_priv(new_state=FAILED)
                return task
    try:
        task_workdir.close()
    except Exception, ex:
        task._log.append(str(ex))
        # don't propagate a 'FAILED' state here

    # Set state to 'Pending'. From here on, BigJob will
    # determine the state of this task.
    task._set_and_propagate_state_change_priv(new_state=PENDING)

    # return the compute unit description
    return task
