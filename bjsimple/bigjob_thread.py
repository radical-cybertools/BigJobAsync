#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

# global includes
import time
import saga
import pilot
import threading

# local includes 
from constants import *

#-----------------------------------------------------------------------------
#
class _BigJobThread(threading.Thread):
    """This class wraps a single BigJob 'PilotCompute' into a thread which
    allows pseudo-asynchronous state callbacks and file transfers.

    We call this a 'BigJob-in-a-thread' (tm).
    """

    #-------------------------------------------------------------------------
    #
    def __init__(self, simple_bigjob_object):
        """Le Constructeur creates a new 'BigJob-in-a-thread' (tm). 
        """
        threading.Thread.__init__(self)
        self.daemon    = True
        self.lock      = threading.Lock()
        self.terminate = threading.Event()

        self.tasks         = list()
        self.pilot_service = None
        self.pilotjob      = None
        self.sbj_obj       = simple_bigjob_object

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the main thread loop in a coordinated fashion.
        """
        self.terminate.set()

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Start the 'BigJob-in-a-thread' (tm)'.

        This is the standard Python thread method that is triggered via 
        thread.start() (Note that it's not thread.run()!).
        """

        # first we try to launch a BigJob
        self._launch_bj()
        start_time = time.time()
        
        # And then we loop until we get interrupted, until the BigJob 
        # finishes or until we hit a time out. 
        while not self.terminate.isSet():

            # sometimes pilot jobs have the tendency not terminate 
            # properly. in this case, we monitor the runtime and terminate
            # manually after the rumtime (+ some grace period) has expired
            if time.time() - start_time >= (self.sbj_obj._runtime + 1)  * 60:
                self.stop()

            # peridically update bigjob state
            if self.sbj_obj.state in [PENDING, RUNNING]:
                self._update_bigjob_state(self.sbj_obj)
            else:
                # causes the main loop to termiante after this iteration
                self.stop()

            self.lock.acquire() # LOCK while working with self.tasks
            for task in self.tasks:
                if task['submitted'] == False:
                    # task needs to be launched
                    self._launch_task(task)
                else:
                    # task was already submitted. check state 
                    if task['task_obj'].state in [PENDING, RUNNING]:
                        # task is in an active state.
                        self._update_task_state(task)
                    else:
                        # task is in a terminal state. do nothing
                        pass
            self.lock.release() # UNLOCK 

            # wait a bit before we update again
            time.sleep(UPDATE_INTERVAL)

        # once we have left the main loop, the only thing left to do
        # is to cancel the pilot job and its service and set the final state
        self.pilot_service.cancel()
        self.sbj_obj._set_and_propagate_state_change_priv(new_state=DONE)

    # ------------------------------------------------------------------------
    #
    def _launch_bj(self):
        """Starts a BigJob on the target machine.
        """
        try: 
            # Try to create the working directory. If This fails, we set the 
            # state of this BigJob to 'Failed'.
            d = saga.filesystem.Directory(self.sbj_obj._remote_workdir_url, 
                saga.filesystem.CREATE_PARENTS)
            d.close()
        except Exception, ex:
            self.sbj_obj._log.append(str(ex))
            self.sbj_obj._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        # Launch the BigJob
        try:
            # Create working directory 

            # Create pilot description
            pilot_description = pilot.PilotComputeDescription()
            pilot_description.service_url         = self.sbj_obj._resource['jobmgr_url']
            pilot_description.number_of_processes = self.sbj_obj._cores
            pilot_description.walltime            = self.sbj_obj._runtime
            if self.sbj_obj._project_id is not None:
                pilot_description.project         = self.sbj_obj._project_id
            if self.sbj_obj._queue == DEFAULT:
                pilot_description.queue           = self.sbj_obj._resource['jobmgr_queue']
            else:
                pilot_description.queue           = self.sbj_obj._queue
            url = saga.Url(self.sbj_obj._resource['shared_fs_url'])
            url.path = self.sbj_obj.workdir
            pilot_description.working_directory   = url.path

            # Connect to REDIS, create Pilot Compute Service
            redis_url = "redis://%s@%s" % (self.sbj_obj._resource['redis_pwd'], 
                self.sbj_obj._resource['redis_host'])
            self.sbj_obj._log.append("Connecting to REDIS server at %s" % \
                self.sbj_obj._resource['redis_host'])
            self.pilot_service = pilot.PilotComputeService(redis_url)

            # Launch Pilot Job
            self.sbj_obj._log.append("Launching Pilot Job: %s" % str(pilot_description))
            self.pilotjob = self.pilot_service.create_pilot(pilot_description)

        except Exception, ex:
            # something went wrong. append the exception to the log 
            # and call the callbacks.
            self.sbj_obj._log.append(str(ex))
            self.sbj_obj._set_and_propagate_state_change_priv(new_state=FAILED)

        self.sbj_obj._set_and_propagate_state_change_priv(new_state=PENDING)

    # ------------------------------------------------------------------------
    #
    def _launch_task(self, task):
        """Converts a task into a CU and submits it to BigJob.
        """
        try:
            wd = "%s/%s" % (self.sbj_obj._workdir, task['task_obj'].dir_name)

            cu_description = pilot.ComputeUnitDescription()
            cu_description.executable        = task['task_obj'].executable
            cu_description.arguments         = task['task_obj'].arguments
            cu_description.working_directory = wd
            cu_description.output            = "STDOUT"
            cu_description.error             = "STDERR"

            comp_unit = self.pilotjob.submit_compute_unit(cu_description)
            task['cu_obj'] = comp_unit

            task['task_obj']._set_and_propagate_state_change_priv(new_state=PENDING)

            task['submitted'] = True

        except Exception, ex:
            task['task_obj']._log.append(str(ex))
            task['task_obj']._set_and_propagate_state_change_priv(new_state=FAILED)
            return -1


    # ------------------------------------------------------------------------
    #
    def _update_bigjob_state(self, sbj_obj):
        """Updates the state of the BigJob.
        """
        try:
            state = self.pilotjob.get_state().lower() 
        except Exception, ex:
            self.sbj_obj._log.append(str(ex))
            self.sbj_obj._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        if state in ['unknown', 'new']:
            translated_state = PENDING
        elif state == 'running':
            translated_state = RUNNING
        elif state == 'done':
            translated_state = DONE
        else:
            error_msg = "BigJob returned state '%s'" % state
            self.sbj_obj._log.append(error_msg)
            translated_state = FAILED

        sbj_obj._set_and_propagate_state_change_priv(translated_state)

    # ------------------------------------------------------------------------
    #
    def _update_task_state(self, task):
        """Updates the state of a task.
        """
        # fuzzy sanity check. 
        if task['cu_obj'] == None:
            return

        try:
            state = task['cu_obj'].get_state().lower() 
        except Exception, ex:
            task['task_obj']._log.append(str(ex))
            task['task_obj']._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        if state in ['unknown', 'new']:
            translated_state = PENDING
        elif state == 'running':
            translated_state = RUNNING
        elif state == 'done':
            translated_state = DONE_WAITING_FOR_TRANSFER
        else:
            error_msg = "BigJob returned CU state '%s'" % state
            task['task_obj']._log.append(error_msg)
            translated_state = FAILED

        task['task_obj']._set_and_propagate_state_change_priv(translated_state)

    # ------------------------------------------------------------------------
    #
    def add_tasks(self, tasks):
        """Adds one or more tasks to the BigJob-in-a-thread.
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        self.lock.acquire() # LOCK while working with self.tasks
        for task in tasks:
            self.tasks.append(
                {
                    'task_obj': task, 
                    'cu_obj': None, 
                    'submitted': False
                })
        self.lock.release() # UNLOCK
