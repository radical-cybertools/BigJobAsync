#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import saga
import time
import Queue
import constants
import subprocess
import multiprocessing

from task import Task

# ----------------------------------------------------------------------------
#
class _InputTransferWorker(multiprocessing.Process):

    # ------------------------------------------------------------------------
    #
    def __init__(self, ready_to_transfer_input_q, ready_to_exec_q,
                 done_q, failed_q):
        """DS
        """
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self._stop  = False

        # All queue an InputFileTransferWorker can access
        self._tasks_done_q = done_q
        self._tasks_failed_q = failed_q
        self._tasks_ready_to_exec_q = ready_to_exec_q
        self._tasks_ready_to_transfer_input_q = ready_to_transfer_input_q

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """DS
        """
        self._stop = True

    # ------------------------------------------------------------------------
    #
    def run(self):
        """DS
        """
        while self._stop is False:
            while True:
                try:
                    task = self._tasks_ready_to_transfer_input_q.get_nowait()
                    # transfer_input_file tries to transfer the input files 
                    # for a given task and puts it afterwards either in the 
                    # 'ready_to_exec' or 'failed' queues. 
                    self.transfer_input_file(task)
                    #self._tasks_ready_to_exec_q.put(task)
                except Queue.Empty:
                    break

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def transfer_input_file(self, task):
        """DOCSTRING
        """
        # Change the task state to 'TransferringInput'
        task._set_state(constants.TRANSFERRING_INPUT)

        # Iterate over the tasks and try to submit them to BigJob after the 
        # input data has been staged. 
        try:
            # create working directories for tasks based on the task uid
            task_workdir_url = "%s/%s" % (task._remote_workdir_url, task.dir_name)
            task._log.append("Creating working directory '%s'" % task.dir_name)

            task_workdir = saga.filesystem.Directory(task_workdir_url, 
                saga.filesystem.CREATE_PARENTS)

        except Exception, ex:
            task._log.append(str(ex))
            task._set_state(constants.FAILED)
            self._tasks_failed_q.put(task)
            return

        # Next we can take care of the file transfers
        for directive in task.input:

            origin      = directive['origin']
            origin_path = directive['origin_path']

            if origin == constants.LOCAL:
                try: 
                    # we use saga-python to copy a local file to the 
                    # remote destination
                    task._log.append("Copying LOCAL input file '%s'" % origin_path)
                    local_filename = "file://localhost//%s" % origin_path
                    local_file = saga.filesystem.File(local_filename)
                    local_file.copy(task_workdir_url)
                    local_file.close()
                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return 

            elif origin == constants.REMOTE:
                try: 
                    # copy around stuff locally on the remote machine
                    task_workdir.copy(origin_path, ".")
                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return

            elif isinstance(origin, Task):
                try: 
                    # this is a hack / workaround until SAGA-Python supports 
                    # linking files.

                    link_host   = saga.Url(task_workdir_url).host
                    link_port   = saga.Url(task_workdir_url).port
                    link_user   = saga.Url(task_workdir_url).username

                    link_source = "%s/%s/%s" % (saga.Url(origin._remote_workdir_url).path, origin._dir_name, origin_path)
                    link_target = "%s/%s" % (saga.Url(task_workdir_url).path, origin_path)

                    link_cmd = "/bin/bash -c \"echo -e 'symlink %s %s' | sftp %s\"" % (link_source, link_target, link_host)
                    task._log.append("Linking input file: %s" % (link_cmd))

                    process = subprocess.Popen(link_cmd, shell=True,
                                               stdout=subprocess.PIPE, 
                                               stderr=subprocess.PIPE)

                    # wait for the process to terminate
                    out, err = process.communicate()
                    errcode = process.returncode

                    if errcode != 0:
                        task._log.append("Linking FAILED: %s" % str(err))
                        task._set_state(constants.FAILED)
                        self._tasks_failed_q.put(task)
                        return 

                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return

            else:
                raise Exception("Invalid paramater for input file origin: %s" % origin)


        try:
            task_workdir.close()
        except Exception, ex:
            task._log.append(str(ex))
            # don't propagate a 'FAILED' state here

        # Set state to 'Pending'. From here on, BigJob will
        # determine the state of this task.
        task._set_state(constants.WAITING_FOR_EXECUTION)
        self._tasks_ready_to_exec_q.put(task)