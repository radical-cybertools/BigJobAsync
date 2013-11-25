from task import * 
import threading
from multiprocessing import Pool

import time
import saga
import pilot




# ----------------------------------------------------------------------------
# RESOURCE DICTIONARY
RESOURCES = {
    'XSEDE.STAMPEDE': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'slurm+ssh://login4.stampede.tacc.utexas.edu',
        'jobmgr_queue'  : 'normal',
        'shared_fs_url' : 'sftp://login4.stampede.tacc.utexas.edu/',
    },

    'FUTUREGRID.ALAMO': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'pbs+ssh://alamo.futuregrid.org',
        'jobmgr_queue'  : 'short',
        'shared_fs_url' : 'sftp://alamo.futuregrid.org/',
    }
}

# ----------------------------------------------------------------------------
# MULTIPROCESSING POOL WORKERS
MAX_WORKERS = 4

# ----------------------------------------------------------------------------
# CONSTANTS
DEFAULT = 'Default'

NEW       = "New"
PENDING   = "Pending"
RUNNING   = "Running"
FAILED    = "Failed"
DONE      = "Done"


#-----------------------------------------------------------------------------
#
class BigJobThread(threading.Thread):

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
        """Tries to terminate the run() loop.
        """
        self.terminate.set()

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Start the thread.
        """

        # first we try to launch a BigJob
        self._launch_bj()
        start_time = time.time()
        
        # And then we loop until we get interrupted 
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

            self.lock.acquire() # FIX -- not the most effective lock
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
            self.lock.release()

            self.sbj_obj._runtime

            time.sleep(1)

        # once we have left the main loop, the only thing left to do
        # is to cancel the pilot job and its service and set the final state
        self.pilot_service.cancel()
        self.sbj_obj._set_and_propagate_state_change_priv(new_state=DONE)


    # ------------------------------------------------------------------------
    #
    def _launch_task(self, task):

        try:
            cu_description = pilot.ComputeUnitDescription()
            cu_description.executable        = task['task_obj'].executable
            cu_description.arguments         = task['task_obj'].arguments
            cu_description.working_directory = "%s/%s" % (self.sbj_obj._workdir, task['task_obj'].dir_name)
            cu_description.output            = "STDOUT"
            cu_description.error             = "STDERR"

            cu = self.pilotjob.submit_compute_unit(cu_description)
            task['cu_obj'] = cu

            task['task_obj']._set_and_propagate_state_change_priv(new_state=PENDING)

            task['submitted'] = True

        except Exception, ex:
            task['task_obj']._log.append(str(ex))
            task['task_obj']._set_and_propagate_state_change_priv(new_state=FAILED)
            return -1


    # ------------------------------------------------------------------------
    #
    def _update_bigjob_state(self, sbj_obj):
        try:
            state = self.pilotjob.get_state().lower() 
        except Exception, ex:
            self.sbj_obj._log.append(str(ex))
            self.sbj_obj._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        if state == 'unknown':
            translated_state = PENDING
        elif state == 'running':
            translated_state = RUNNING
        elif state == 'done':
            translated_state = DONE
        else:
            translated_state = FAILED

        sbj_obj._set_and_propagate_state_change_priv(translated_state)

    # ------------------------------------------------------------------------
    #
    def _update_task_state(self, task):

        # fuzzy sanity check. 
        if task['cu_obj'] == None:
            return

        try:
            state = task['cu_obj'].get_state().lower() 
        except Exception, ex:
            task['task_obj']._log.append(str(ex))
            task['task_obj']._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        if state == 'unknown':
            translated_state = PENDING
        elif state == 'running':
            translated_state = RUNNING
        elif state == 'done':
            translated_state = DONE
        else:
            translated_state = FAILED

        task['task_obj']._set_and_propagate_state_change_priv(translated_state)


    # ------------------------------------------------------------------------
    #
    def add_tasks(self, tasks):
        """Adds one or more tasks to the BigJob-in-a-thread.
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        self.lock.acquire()
        for task in tasks:
            self.tasks.append(
                {
                    'task_obj': task, 
                    'cu_obj': None, 
                    'submitted': False
                })
        self.lock.release()

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
            self.sbj_obj._log.append("Connecting to REDIS server at %s" % self.sbj_obj._resource['redis_host'])
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


# ----------------------------------------------------------------------------
# 
def _compute_unit_launcher_worker(remote_work_dir_url, task):
    # we use a single directory instance that we pass around for 
    # performance reasons as it keeps the connection open. 
    remote_workdir = saga.filesystem.Directory(remote_work_dir_url)

    # next, we iterate over the tasks and try to submit them to BigJob 
    # after the input data has been staged. 
    try:
        # create working directories for tasks based on the task uid
        task._log.append("Creating working directory '%s'" % task.dir_name)
        task_workdir = remote_workdir.open_dir(task.dir_name, saga.filesystem.CREATE)
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
                local_file.copy(task_workdir.url)
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

    task_workdir.close()
    remote_workdir.close()

    # Set state to 'Pending'. From here on, BigJob will
    # determine the state of this task.
    task._set_and_propagate_state_change_priv(new_state=PENDING)

    # return the compute unit description
    return task








# ----------------------------------------------------------------------------
#
class BigJobSimple(object):



    # ------------------------------------------------------------------------
    #
    def __init__(self, name, resource, runtime, cores, workdir, project_id=None, queue=DEFAULT):
        """Creates a new BigJob instance.
        """
        self._cbs = []
        self._log = []
        self._tasks = []

        self._name = name
        self._resource = resource
        self._runtime = runtime
        self._cores = cores
        self._project_id = project_id
        self._workdir = workdir
        self._queue = queue

        self._cus = []
        self._pilot_job = None
        self._state = NEW

        # The URL of the working directorty.
        self._remote_workdir_url = "%s/%s/" % (self._resource['shared_fs_url'], self.workdir)


        # BigJob-in-a-thread
        self.bj_thread = BigJobThread(self)


        # The worker pool handles asynchronous interaction with BigJob 

        self.pool = Pool(processes=MAX_WORKERS)  

    # ------------------------------------------------------------------------
    #
    def __str__(self):
        return self._name

    # ------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the full log.
        """
        return self._log

    # ------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the state.
        """
        return self._state


    # ------------------------------------------------------------------------
    #
    @property
    def workdir(self):
        """Returns the working directory.
        """
        return self._workdir 

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

        self._cbs.extend(callbacks)

    # ------------------------------------------------------------------------
    #
    def allocate(self):
        """Tries to allocate the requested resource by starting a BigJob
        on the target machine.
        """

        # All we really do here is to start BigJob-in-a-thread
        self.bj_thread.start()
        

    # ------------------------------------------------------------------------
    #
    def wait(self):
        """Waits...
        """
        self.bj_thread.join()

    # ------------------------------------------------------------------------
    #
    def schedule_tasks(self, tasks):
        """Schedules one or more tasks for execution.
        """
        if not isinstance(tasks, list):
            tasks = [tasks] 
        self._tasks.extend(tasks)

        results = []

        for task in tasks:
            # register a callback on the task that will add it to BigJob 
            # once it has reached a pending state
            t = _compute_unit_launcher_worker(self._remote_workdir_url, task)
            #results.append(result)

        #for r in results:
         #   task = r.get() ## THIS IS NOT OPTIMAL
            self.bj_thread.add_tasks(t)

    # ------------------------------------------------------------------------
    #
    def _set_and_propagate_state_change_priv(self, new_state):
        """Propagate a state change to all callback functions.
        """
        # do nothing if existing and new state are identical
        if self._state == new_state:
            return

        old_state = self._state
        self._state = new_state

        for callback in self._cbs:
            callback(self, old_state, new_state)


