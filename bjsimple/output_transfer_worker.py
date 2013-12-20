#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os 
import saga
import time
import json
import Queue
import constants
import multiprocessing

from cgi import parse_qs

# ----------------------------------------------------------------------------
#
class _OutputTransferWorker(multiprocessing.Process):

    # ------------------------------------------------------------------------
    #
    def __init__(self, ready_to_transfer_output_q, done_q, failed_q):
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self._stop  = False

        # All queue an OutputFileTransferWorker can access
        self._tasks_done_q = done_q
        self._tasks_failed_q = failed_q
        self._tasks_ready_to_transfer_output_q = ready_to_transfer_output_q

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
                    task = self._tasks_ready_to_transfer_output_q.get_nowait()
                    # transfer_input_file tries to transfer the input files 
                    # for a given task and puts it afterwards either in the 
                    # 'ready_to_exec' or 'failed' queues. 
                    self.transfer_output_file(task)
                    #self._tasks_ready_to_exec_q.put(task)
                except Queue.Empty:
                    break

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def transfer_output_file(self, task):

        # Change the task state to 'TransferringInput'
        task._set_state(constants.TRANSFERRING_OUTPUT)

        # Iterate over the tasks and try to submit them to BigJob after the 
        # input data has been staged. 
        try:
            # create working directories for tasks based on the task uid

            task_workdir_url = saga.Url("%s/%s" % (task._remote_workdir_url, task.dir_name))
            task_workdir_url.path = os.path.abspath(task_workdir_url.path)

            task_workdir = saga.filesystem.Directory(task_workdir_url)

        except Exception, ex:
            task._log.append(str(ex))
            task._set_state(constants.FAILED)
            self._tasks_failed_q.put(task)
            return

                # Next we can take care of the file transfers
        for directive in task.output:

            origin_path      = directive['origin_path']
            destination      = directive['destination']
            destination_path = directive['destination_path']

            try: 
                output_file_url = "%s/%s" % (task_workdir_url, origin_path)
                
                if destination == constants.LOCAL:
                    # copying REMOTE -> LOCAL
                    if destination_path == '.':
                        local_path = os.getcwd()
                    else:
                        if destination_path.startswith("/") is False:
                            local_path = "%s/%s" % (os.getcwd(), destination_path)
                        else:
                            local_path = destination_path

                    local_filename = "file://localhost//%s" % local_path
                    task_workdir.copy(output_file_url, local_filename)
                    task._log.append("Copying output file %s to %s" % (output_file_url, local_filename))

                    print "Copying output file %s to %s" % (output_file_url, local_filename)

                elif destination == constants.REMOTE:
                    # copying REMOTE -> REMOTE                    
                    task_workdir.copy(output_file_url, destination_path)
                    task._log.append("Copying output file %s to %s" % (output_file_url, destination_path))

                else:
                    raise Exception("Invalid paramater for output file destination: %s" % destination)

            except Exception, ex:
                task._log.append(str(ex))
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
        task._set_state(constants.DONE)
        self._tasks_done_q.put(task)
