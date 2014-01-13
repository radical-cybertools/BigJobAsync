#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import saga
import time
import pilot
import Queue
import constants
import multiprocessing

# ----------------------------------------------------------------------------
#
class _BigJobWorker(multiprocessing.Process):

    # ------------------------------------------------------------------------
    #
    def __init__(self, resource_obj, ready_to_transfer_input_queue, 
        ready_to_exec_q, ready_to_transfer_output_q, done_q, failed_q):
        """DS
        """
        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self._stop  = False

        # The resource object binds the worker to the public API & callbacks
        self._res_obj = resource_obj

        # BigJob handles
        self._pilot_job = None
        self._pilot_service = None

        self._physical_tasks = []

        # All queue an InputFileTransferWorker can access
        self._tasks_done_q = done_q
        self._tasks_failed_q = failed_q
        self._tasks_ready_to_exec_q = ready_to_exec_q
        self._tasks_ready_to_transfer_output_q = ready_to_transfer_output_q
        self._tasks_ready_to_transfer_input_q = ready_to_transfer_input_queue


    # ------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the resource log.
        """
        return self._res_obj['log']

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
        start_time = time.time()
        # First of all, the BigJobWorker needs to launch a BigJob 
        # instance on which it can schedule tasks that come in via 
        # the _tasks_ready_to_exec_q queue.
        self._launch_bj()

        while self._stop is False:

            # Sometimes pilot jobs have the tendency not terminate 
            # properly. in this case, we monitor the runtime and terminate
            # manually after the rumtime (+ some grace period) has expired
            if time.time() - start_time >= (self._res_obj['runtime'] + 1)  * 60:
                self.stop()
                continue

            # Periodically, we check the status of our BigJob pilot object,
            # translate the state and call the state change callbacks.
            if self._res_obj['state'] not in [constants.DONE, constants.FAILED]:
                # Obviously, we only do this if the object is not in a 
                # terminal state, i.e., 'Done' or 'Failed'.
               self._update_bj()

            # Periodically, we check the 'ready to execute queue'. If there's 
            # something in it, we convert it into a CU and launch it 'into'
            # the BigJob pilot. 
            try: 
                task = self._tasks_ready_to_exec_q.get_nowait()
                # New task ready to execute. Add to internal task list
                self._physical_tasks.append({'task': task, 'cu': None})
                #self._tasks_ready_to_transfer_output_q.put(task)
            except Queue.Empty:
                pass

            # Periodically, we check the states of all running jobs, update 
            # and push things into the appropriate queues accordingly.
            for pt in self._physical_tasks:

                self._update_task_state(pt)

                if pt['task'].state == constants.WAITING_FOR_EXECUTION:
                    pt['cu'] = self._schedule_cu(pt['task'])

                elif pt['task'].state in [constants.WAITING_FOR_OUTPUT_TRANSFER]:
                    self._tasks_ready_to_transfer_output_q.put(pt['task'])
                    self._physical_tasks.remove(pt)

                    self._tasks_ready_to_exec_q.task_done()

                elif pt['task'].state in [constants.DONE]:
                    # Task is done, i.e., there are no output files to
                    # transfer. Remove it. 
                    self._tasks_done_q.put(pt['task'])
                    self._physical_tasks.remove(pt)

                    self._tasks_ready_to_exec_q.task_done()

                elif pt['task'].state in [constants.FAILED]:
                    # Task has failed, so there's not much we can do except for 
                    # removing it from the list of physical tasks
                    self._tasks_failed_q.put(pt['task'])
                    self._physical_tasks.remove(pt)

                    self._tasks_ready_to_exec_q.task_done()

                elif pt['task'].state in [constants.PENDING]:
                    # Task has been started but is still pending execution.
                    # Not much to do at this point.
                    pass

        # once we have left the main loop, we can cancel everything.
        self._pilot_service.cancel()

    # ------------------------------------------------------------------------
    #
    def _update_task_state(self, task):
        """DOCSTRING
        """
        if task['cu'] is None:
            # Task has no CU associated with it yet. Not much we can do. 
            return 

        else:
            try:
                new_cu_state = task['cu'].get_state().lower()
            except Exception, ex:
                task['task']._log.append(str(ex))
                task['task']._set_state(constants.FAILED)
                return

            if new_cu_state in ['unknown', 'new']:
                translated_state = constants.PENDING
            elif new_cu_state == 'running':
                translated_state = constants.RUNNING
            elif new_cu_state == 'done':
                if len(task['task'].output) > 0:
                    translated_state = constants.WAITING_FOR_OUTPUT_TRANSFER
                else:
                    translated_state = constants.DONE
            else:
                error_msg = "BigJob returned CU state '%s'" % new_cu_state
                task['task']._log.append(error_msg)
                translated_state = constants.FAILED

            task['task']._set_state(translated_state)

    # ------------------------------------------------------------------------
    #
    def _set_state(self, new_state):
        """Starts a BigJob on the target machine.
        """
        # do nothing if existing and new state are identical
        if self._res_obj['state'] == new_state:
            return

        old_state = self._res_obj['state']
        self._res_obj['state'] = new_state

        for callback in self._res_obj['callbacks']:
            callback(self, old_state, new_state)

    # ------------------------------------------------------------------------
    #
    def _schedule_cu(self, task):

        try:
            wd = "%s/%s" % (self._res_obj['workdir'], task.dir_name)

            cu_description = pilot.ComputeUnitDescription()
            cu_description.executable          = task.executable
            cu_description.arguments           = task.arguments
            cu_description.environment         = task.environment
            cu_description.working_directory   = wd
            cu_description.number_of_processes = task.cores
            cu_description.output              = "STDOUT"
            cu_description.error               = "STDERR"

            comp_unit = self._pilot_job.submit_compute_unit(cu_description)
            task._set_state(constants.PENDING)
            return comp_unit

        except Exception, ex:
            task._log.append(str(ex))
            task._set_state(constants.FAILED)
            return None

            #task._set_state(constants.RUNNING)
            #time.sleep(1)
            #task._set_state(constants.WAITING_FOR_OUTPUT_TRANSFER)

            #self._ready_to_transfer_output_queue.put(task)


    # ------------------------------------------------------------------------
    #
    def _launch_bj(self):
        """Starts a BigJob on the target machine.
        """
        try: 
            # Try to create the working directory. If This fails, we set 
            # our state to 'Failed'.
            d = saga.filesystem.Directory(self._res_obj['remote_workdir_url'], saga.filesystem.CREATE_PARENTS)
            d.close()
        except Exception, ex:
            self._res_obj['log'].append(str(ex))
            self._set_state(constants.FAILED)
            return

        try:
            # Create pilot description & launch the BigJob
            pilot_description = pilot.PilotComputeDescription()

            # we construct a service url as username@host
            service_url = saga.Url(self._res_obj['resource']['jobmgr_url'])
            if self._res_obj['username'] is not None:
                service_url.set_username(self._res_obj['username'] )
            service_url = str(service_url)

            pilot_description.service_url         = service_url
            pilot_description.number_of_processes = self._res_obj['cores']
            pilot_description.walltime            = self._res_obj['runtime']
            if self._res_obj['project_id'] is not None:
                pilot_description.project         = self._res_obj['project_id']
            if self._res_obj['queue'] == constants.DEFAULT:
                pilot_description.queue           = self._res_obj['resource']['jobmgr_queue']
            else:
                pilot_description.queue           = self._res_obj['queue']
            url = saga.Url(self._res_obj['resource']['shared_fs_url'])
            url.path = self._res_obj['workdir']
            pilot_description.working_directory   = url.path

            if 'spmd_variation' in self._res_obj['resource']:
                pilot_description.spmd_variation = self._res_obj['resource']['spmd_variation']

            # Connect to REDIS, create Pilot Compute Service
            redis_url = "redis://%s@%s" % (
                self._res_obj['resource']['redis_pwd'], 
                self._res_obj['resource']['redis_host']
            )
            self._res_obj['log'].append("Connecting to REDIS server at %s" % \
                self._res_obj['resource']['redis_host'])
            self._pilot_service = pilot.PilotComputeService(redis_url)

            # Launch Pilot Job
            self._res_obj['log'].append("Launching Pilot Job: %s" % str(pilot_description))

            self._pilot_job = self._pilot_service.create_pilot(pilot_description)

        except Exception, ex:
            # something went wrong. append the exception to the log 
            # and call the callbacks.
            self._res_obj['log'].append(str(ex))
            self._set_state(constants.FAILED)

        self._set_state(constants.PENDING)

    # ------------------------------------------------------------------------
    #
    def _update_bj(self):
        try:
            state = self._pilot_job.get_state().lower() 
        except Exception, ex:
            self._res_obj['log'].append(str(ex))
            self._set_state(constants.FAILED)
            return

        # Translate BigJob states into our own states.
        if state in ['unknown', 'new']:
            translated_state = constants.PENDING
        elif state == 'running':
            translated_state = constants.RUNNING
        elif state == 'done':
            translated_state = constants.DONE
        else:
            error_msg = "BigJob returned state '%s'" % state
            self._res_obj['log'].append(error_msg)
            translated_state = constants.FAILED

        self._set_state(translated_state)
