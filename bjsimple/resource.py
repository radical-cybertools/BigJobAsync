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
import threading 

import constants

import multiprocessing, Queue

# ----------------------------------------------------------------------------
#
class BigJobWorker(multiprocessing.Process):

    # ------------------------------------------------------------------------
    #
    def __init__(self, resource_obj, ready_to_exec_q, 
        ready_to_transfer_output_q, done_q, failed_q):
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

                elif pt['task'].state in [constants.DONE]:
                    # Task is done, i.e., there are no output files to
                    # transfer. Remove it. 
                    self._tasks_done_q.put(pt['task'])
                    self._physical_tasks.remove(pt)

                elif pt['task'].state in [constants.FAILED]:
                    # Task has failed, so there's not much we can do except for 
                    # removing it from the list of physical tasks
                    self._tasks_failed_q.put(pt['task'])
                    self._physical_tasks.remove(pt)

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
            cu_description.executable        = task.executable
            cu_description.arguments         = task.arguments
            cu_description.working_directory = wd
            cu_description.output            = "STDOUT"
            cu_description.error             = "STDERR"

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
            d = saga.filesystem.Directory(self._res_obj['remote_workdir_url'], 
                saga.filesystem.CREATE_PARENTS)
            d.close()
        except Exception, ex:
            self._res_obj['log'].append(str(ex))
            self._set_state(constants.FAILED)
            return

        try:
            # Create pilot description & launch the BigJob
            pilot_description = pilot.PilotComputeDescription()
            pilot_description.service_url         = self._res_obj['resource']['jobmgr_url']
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


# ----------------------------------------------------------------------------
#
class InputFileTransferWorker(multiprocessing.Process):

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

    def stop(self):
        """DS
        """
        self._stop = True

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

            if directive['location'] == constants.LOCAL:
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
                    task._set_state(constants.FAILED)
                    self._tasks_failed_q.put(task)
                    return 

            elif directive['location'] == constants.REMOTE:
                try: 
                    # copy around stuff locally on the remote machine
                    task._log.append("Copying REMOTE input file '%s'" % directive['path'])
                    task_workdir.copy(directive['path'], ".")
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
        task._set_state(constants.WAITING_FOR_EXECUTION)
        self._tasks_ready_to_exec_q.put(task)


# ----------------------------------------------------------------------------
#
class OutputFileTransferWorker(multiprocessing.Process):

    def __init__(self, ready_to_transfer_output_q, done_q, failed_q):
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self._stop  = False

        # All queue an OutputFileTransferWorker can access
        self._tasks_done_q = done_q
        self._tasks_failed_q = failed_q
        self._tasks_ready_to_transfer_output_q = ready_to_transfer_output_q

    def stop(self):
        """DS
        """
        self._stop = True

    def run(self):
        """DS
        """
        while self._stop is False:
            while True:
                try:
                    task = self._tasks_ready_to_transfer_output_q.get_nowait()
                    print "OutputFileTransferWorker: %s" % task
                    self._tasks_done_q.put(task)
                except Queue.Empty:
                    break

            time.sleep(1)

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
            iftw = InputFileTransferWorker(
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
            oftw = OutputFileTransferWorker(
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
        self._bjw = BigJobWorker(
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

            # peridically update bigjob state
            #if self._state in [constants.PENDING, constants.RUNNING]:
            #    self._update_bj()
            #else:
                # this main loop terminates in case the pilot job 
                # has reached a terminal state.
            #    self.stop()

            # periodically query the queue of tasks that are ready to 
            # execute. add all tasks that are ready to big job
            #tasks_to_run = []
            #while True:
            #    try:
            #        tasks_to_run.append(self._ready_to_execute_queue.get_nowait())
            #    except Queue.Empty, qex:
            #        break
            # submit stuff to BigJob -- much faster in bulk! 
            #elf._schedule_cus(tasks_to_run)

            # and wait for a bit
            time.sleep(constants.UPDATE_INTERVAL)

        # finally, we need to shut down the pilot job itself.
        #self._cancel_bj()

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

