#!/usr/bin/env python

"""The resource representation / interface in BigJobAsync.
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

import time
import math
import pilot
import saga
import Queue
import threading
import multiprocessing 

import constants

from logger import logger

from input_transfer_worker  import _InputTransferWorker
from output_transfer_worker import _OutputTransferWorker


# ----------------------------------------------------------------------------
#
class Resource(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, name, resource, runtime, cores, workdir, 
        username=None, project_id=None, queue=constants.DEFAULT):
        """Le Constructeur creates a resource new instance.
        """
        threading.Thread.__init__(self)
        self.daemon     = True
        self.lock       = threading.Lock()
        self._terminate = threading.Event()

        if "core_increment" in resource:
            # round up to the nearest core increment
            cores = math.ceil(cores/resource["core_increment"])
            logger.info("Rounding up allocation to nearest core increment: %s." % (cores))

        self._resource_obj                       = {}
        self._resource_obj['log']                = []
        self._resource_obj['callbacks']          = []
        self._resource_obj['state']              = constants.NEW
        self._resource_obj['name']               = name
        self._resource_obj['username']           = username 
        self._resource_obj['resource']           = resource
        self._resource_obj['workdir']            = workdir
        self._resource_obj['runtime']            = runtime
        self._resource_obj['cores']              = cores
        self._resource_obj['project_id']         = project_id
        self._resource_obj['queue']              = queue

        # make sure the backends we need are avaialbe 
        from bigjobasync import USE_SAGA_PILOT
        if USE_SAGA_PILOT is True:
            try: 
                import sinon
            except ImportError:
                raise Exception("Couldn't find SAGA-Pilot. Please install first via 'pip install --upgrade -e git://github.com/saga-project/saga-pilot.git@master#egg=saga-pilot'.")
        else:
            try:
                import pilot
            except ImportError:
                raise Exception("Couldn't find BigJob. Please install first via 'pip install --upgrade bigjob'.")

        # make sure we have at least version0.9.16 of saga-python
        if saga.version < "0.10":
            raise Exception("Need saga-python >= 0.9.16. Found %s. Please update via 'pip install --upgrade saga-python'." % saga.version)

        # inject username into remote_workdir_url
        remote_workdir_url = saga.Url("%s/%s/" % (resource['shared_fs_url'], workdir))
        if username is not None:
            remote_workdir_url.set_username(username)
        remote_workdir_url = str(remote_workdir_url)
        self._resource_obj['remote_workdir_url'] = remote_workdir_url

        self._terminate_on_empty_queue = False

        self._ready_to_transfer_input_queue = multiprocessing.JoinableQueue()
        self._ready_to_execute_queue = multiprocessing.JoinableQueue()
        self._ready_to_transfer_output_queue = multiprocessing.JoinableQueue()
        self._done_queue = multiprocessing.JoinableQueue()
        self._failed_queue = multiprocessing.JoinableQueue()

        self._iftws = []
        for x in range(0, constants.MAX_INPUT_TRANSFER_WORKERS):
            iftw = _InputTransferWorker(
                wid=x+1,
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
                wid=x+1,
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
    def allocate(self, terminate_on_empty_queue=False):
        """Tries to allocate the requested resource by starting a BigJob agent
        on the target machine.


        If terminate_on_empty_queue=True, the resource will be shut down
        as soon the last task has finished. 
        """
        self._terminate_on_empty_queue = terminate_on_empty_queue

        # Here we start the BigJob or SAGA-Pilot worker. 
        from bigjobasync import USE_SAGA_PILOT
        if USE_SAGA_PILOT is True:
            logger.info("Using SAGA-Pilot for resource and task management.")

            from bigjobasync.saga_pilot_worker import _SAGAPilotWorker
            self._bjw = _SAGAPilotWorker(
                resource_obj=self._resource_obj,
                ready_to_transfer_input_queue=self._ready_to_transfer_input_queue,
                ready_to_exec_q=self._ready_to_execute_queue,
                ready_to_transfer_output_q=self._ready_to_transfer_output_queue,
                done_q=self._done_queue,
                failed_q=self._failed_queue,
            )

        else: 
            logger.info("Using BigJob for resource and task management.")

            from bigjobasync.big_job_worker import _BigJobWorker
            self._bjw = _BigJobWorker(
                resource_obj=self._resource_obj,
                ready_to_transfer_input_queue=self._ready_to_transfer_input_queue,
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
        self._ready_to_transfer_input_queue.join()
        self._ready_to_execute_queue.join()
        self._ready_to_transfer_output_queue.join()

        if self._terminate_on_empty_queue is False:
            # wait until the BJ runs out of queue time
            self._bjw.join()
        else:
            # terminate bigjob
            self._bjw.stop()

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

