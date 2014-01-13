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
                    self._tasks_ready_to_transfer_input_q.task_done()

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

            mode        = directive['mode']
            origin      = directive['origin']
            origin_path = directive['origin_path']

            ####################################################################
            #
            # COPY LOCAL TO REMOTE FILE
            if origin == constants.LOCAL:

                if mode == constants.LINK:
                    task._log.append("Mode '%s' is not supported for local-to-remote transfers." % mode)
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return

                elif mode == mode == constants.COPY:
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

                else:
                    raise task._log.append("Unsupported transfer mode '%s'" % mode)
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return

            ####################################################################
            #
            # COPY / LINK REMOTE TO REMOTE FILE
            elif origin == constants.REMOTE:
                try: 
                    if mode == constants.COPY:
                        # copy around stuff locally on the remote machine
                        task_workdir.copy(origin_path, ".")
                    elif mode == constants.LINK: 
                        # link stuff instead of copying it
                        task_workdir.link(origin_path, ".")
                    else:
                        raise Exception("Unsupported transfer mode '%s'" % mode)

                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return 

            ####################################################################
            #
            # COPY / LINK TASK OUTPUT TO OTHER 
            elif isinstance(origin, Task):
                try: 
                    source = "%s/%s/%s" % (saga.Url(origin._remote_workdir_url).path, origin._dir_name, origin_path)

                    if mode == constants.COPY:
                        task._log.append("Copying REMOTE input file '%s'" % source)
                        task_workdir.copy(source, ".")

                    elif mode == constants.LINK:
                        task._log.append("Linking REMOTE input file '%s'" % source)
                        task_workdir.link(source, ".")
                    else: 
                        raise task._log.append("Unsupported transfer mode '%s'" % mode)

                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return

            ####################################################################
            # UNSUPPORTED ORIGIN TYPE
            else:
                raise task._log.append("Unsupported origin type '%s'" % origin)
                task._set_state(constants.FAILED)
                self._tasks_failed_q.put(task)
                return  


        try:
            task_workdir.close()
        except Exception, ex:
            task._log.append(str(ex))
            # don't propagate a 'FAILED' state here

        # Set state to 'Pending'. From here on, BigJob will
        # determine the state of this task.
        task._set_state(constants.WAITING_FOR_EXECUTION)
        self._tasks_ready_to_exec_q.put(task)
