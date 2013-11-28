#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import time
import pilot
import saga
import Queue
import threading
import multiprocessing 

import constants

from big_job_worker         import _BigJobWorker
from input_transfer_worker  import _InputTransferWorker
from output_transfer_worker import _OutputTransferWorker


# ----------------------------------------------------------------------------
#
class Resource(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, name, resource, runtime, cores, workdir, 
        project_id=None, queue=constants.DEFAULT):
        """Le Constructeur creates a resource new instance.
        """
        threading.Thread.__init__(self)
        self.daemon     = True
        self.lock       = threading.Lock()
        self._terminate = threading.Event()

        self._resource_obj = {}
        self._resource_obj['log'] = []
        self._resource_obj['callbacks'] = []
        self._resource_obj['state'] = constants.NEW
        self._resource_obj['name'] = name
        self._resource_obj['resource'] = resource
        self._resource_obj['workdir'] = workdir
        self._resource_obj['runtime'] = runtime
        self._resource_obj['cores'] = cores
        self._resource_obj['project_id'] = project_id
        self._resource_obj['queue'] = queue
        self._resource_obj['remote_workdir_url'] = "%s/%s/" % \
         (resource['shared_fs_url'], workdir)

        self._ready_to_transfer_input_queue = multiprocessing.Queue()
        self._ready_to_execute_queue = multiprocessing.Queue()
        self._ready_to_transfer_output_queue = multiprocessing.Queue()
        self._done_queue = multiprocessing.Queue()
        self._failed_queue = multiprocessing.Queue()

        self._iftws = []
        for x in range(0, constants.MAX_INPUT_TRANSFER_WORKERS):
            iftw = _InputTransferWorker(
                ready_to_transfer_input_q=self._ready_to_transfer_input_queue,
                ready_to_exec_q=self._ready_to_execute_queue,
                done_q=self._done_queue,
                failed_q=self._failed_queue,
            )
            iftw.start()
            self._iftws.append(iftw)

        self._bjw = None 

        self._oftws = []
        for x in range(0, constants.MAX_OUTPUT_TRANSFER_WORKERS):
            oftw = _OutputTransferWorker(
                ready_to_transfer_output_q=self._ready_to_transfer_output_queue,
                done_q=self._done_queue,
                failed_q=self._failed_queue,
            )
            oftw.start()
            self._oftws.append(oftw)

    # ------------------------------------------------------------------------
    #
    def register_callbacks(self, callbacks):
        """Registers one or more new callback function(s). 

        Callbacks are called whenever a BigJob object makes a state transition.
        Possible states are:

            * NEW
            * PENDING
            * RUNNING
            * FAILED
            * DONE 
        """
        if not isinstance(callbacks, list):
            callbacks = [callbacks] 

        self._resource_obj['callbacks'].extend(callbacks)

    # ------------------------------------------------------------------------
    #
    def allocate(self):
        """Tries to allocate the requested resource by starting a BigJob agent
        on the target machine.
        """

        # Here we start the BigJob worker. 
        self._bjw = _BigJobWorker(
            resource_obj=self._resource_obj,
            ready_to_exec_q=self._ready_to_execute_queue,
            ready_to_transfer_output_q=self._ready_to_transfer_output_queue,
            done_q=self._done_queue,
            failed_q=self._failed_queue,
        )
        self._bjw.start()

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the main thread loop in a coordinated fashion.
        """
        self._bjw.stop()

    # ------------------------------------------------------------------------
    #
    def wait(self):
        """Waits for the resource to reach a terminal state.
        """
        self._bjw.join()

    # ------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the resource log.
        """
        return self._resource_obj['log']

    # ------------------------------------------------------------------------
    #
    def schedule_tasks(self, tasks):
        """Schedules one or more tasks for execution.
        """
        if not isinstance(tasks, list):
            tasks = [tasks] 

        for task in tasks:
            task._remote_workdir_url = self._resource_obj['remote_workdir_url']
            self._ready_to_transfer_input_queue.put(task)

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the tread. Should not be called directyl but 
        via 'allocate()'.
        """

        # first we try to launch a BigJob
        #self._launch_bj()
        start_time = time.time()

        while not self._terminate.isSet():
            # sometimes pilot jobs have the tendency not terminate 
            # properly. in this case, we monitor the runtime and terminate
            # manually after the rumtime (+ some grace period) has expired
            if time.time() - start_time >= (self._resource_obj['runtime'] + 1)  * 60:
                self.stop()

            # and wait for a bit
            time.sleep(constants.UPDATE_INTERVAL)

        self._bjw.terminate()

    # ------------------------------------------------------------------------
    #
    def _set_state(self, new_state):
        # do nothing if existing and new state are identical
        if self._resource_obj['state'] == new_state:
            return

        old_state = self._resource_obj['state']
        self._resource_obj['state'] = new_state

        for callback in self._resource_obj['callbacks']:
            callback(self, old_state, new_state)

