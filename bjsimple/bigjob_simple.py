import saga
import pilot

from task import * 

# ----------------------------------------------------------------------------
# RESOURCE DICTIONARY
RESOURCES = {
    'XSEDE.STAMPEDE': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'slurm+ssh://login1.stampede.tacc.utexas.edu',
        'jobmgr_queue'  : 'normal',
        'shared_fs_url' : 'sftp://login1.stampede.tacc.utexas.edu/',
    }
}


# ----------------------------------------------------------------------------
# CONSTANTS
DEFAULT = 'Default'

NEW     = "New"
PENDING = "Pending"
RUNNING = "Running"
FAILED  = "Failed"
DONE    = "Done"

# ----------------------------------------------------------------------------
#
class BigJobSimple(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, resource, runtime, cores, project_id, workdir, queue=DEFAULT):
        """Creates a new BigJob instance.
        """
        self._cbs = []
        self._log = []
        self._tasks = []

        self._resource = resource
        self._runtime = runtime
        self._cores = cores
        self._project_id = project_id
        self._workdir = workdir
        self._queue = queue

        self._pilot_job = None
        self._state = NEW

        # The URL of the working directorty.
        self._remote_workdir_url = "%s/%s/" % (self._resource['shared_fs_url'], self.workdir)

    # ------------------------------------------------------------------------
    #
    def __str__(self):
        return "ME!"

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

        # Try to create the working directory. If This fails, we set the 
        # state of this BigJob to 'Failed'.
        try: 
            saga.filesystem.Directory(self._remote_workdir_url, saga.filesystem.CREATE_PARENTS)
        except Exception, ex:
            self._log.append(str(ex))
            self._set_and_propagate_state_change_priv(new_state=FAILED)
            return

        # Launch the BigJob
        self._launch_pj_priv()
        self._set_and_propagate_state_change_priv(new_state=PENDING)

    # ------------------------------------------------------------------------
    #
    def wait(self):
        """Waits...
        """
        self._set_and_propagate_state_change_priv(new_state=DONE)

    # ------------------------------------------------------------------------
    #
    def schedule_tasks(self, tasks):
        """Schedules one or more tasks for execution.
        """
        if not isinstance(tasks, list):
            tasks = [tasks] 
        self._tasks.extend(tasks)

        # we use one global directory instance for performance reasons
        # as it keeps the connection open. 
        remote_workdir = saga.filesystem.Directory(self._remote_workdir_url)

        for task in self._tasks:

            try:
                # create working directories for tasks based on the task uid
                task._log.append("Creating working directory '%s'" % task.dir_name)
                task_workdir = remote_workdir.open_dir(task.dir_name, saga.filesystem.CREATE)
            except Exception, ex:
                task._log.append(str(ex))
                task._set_and_propagate_state_change_priv(new_state=FAILED)
                continue

            # next we can take care of the file transfers
            self._do_file_transfer_in(task, task_workdir)

            # Set state to 'Pending'. From here on, BigJob will
            # determine the state of this task.
            task._set_and_propagate_state_change_priv(new_state=PENDING)

        remote_workdir.close()


    # ------------------------------------------------------------------------
    #
    def _do_file_transfer_in(self, task, task_workdir):
        """Takes care of file staging.

        Directives look like this::

            {
                "type" : bjsimple.LOCAL_FILE,  "mode": bjsimple.COPY, 
                "origin" : "/Users/oweidner/Work/Data/test/loreipsum_pt1.txt"
            }

        """
        # Do nothing if there's not input defined
        if len(task._input) < 1:
            return 

        # Change the state to 'TransferInput'
        task._set_and_propagate_state_change_priv(new_state=TRANSFER_INPUT)

        for directive in task._input:
            if directive['type'] == LOCAL_FILE:
                try: 
                    # we use saga-python to copy a local file to the 
                    # remote destination
                    task._log.append("Copying LOCAL input file '%s'" % directive['origin'])
                    local_filename = "file://localhost//%s" % directive['origin']
                    local_file = saga.filesystem.File(local_filename)
                    local_file.copy(task_workdir.url)
                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_and_propagate_state_change_priv(new_state=FAILED)
                    return

            elif directive['type'] == REMOTE_FILE:
                try: 
                    # copy around stuff locally on the remote machine
                    task._log.append("Copying REMOTE input file '%s'" % directive['origin'])
                    task_workdir.copy(directive['origin'], ".")
                    local_file.copy(task_workdir.url)
                except Exception, ex:
                    task._log.append(str(ex))
                    task._set_and_propagate_state_change_priv(new_state=FAILED)
                    return

    # ------------------------------------------------------------------------
    #
    def _launch_pj_priv(self):
        """Create the base working directory and launches a BigJob.
        """
        try:
            # Create working directory 

            # Create pilot description
            pilot_description = pilot.PilotComputeDescription()
            pilot_description.service_url         = self._resource['jobmgr_url']
            pilot_description.number_of_processes = self._cores
            pilot_description.walltime            = self._runtime
            pilot_description.project             = self._project_id
            if self._queue == DEFAULT:
                pilot_description.queue           = self._resource['jobmgr_queue']
            else:
                pilot_description.queue           = self._queue
            url = saga.Url(self._resource['shared_fs_url'])
            url.path = self.workdir
            pilot_description.working_directory   = url.path

            # Connect to REDIS, create Pilot Compute Service
            redis_url = "redis://%s@%s" % (self._resource['redis_pwd'], 
                self._resource['redis_host'])
            self._log.append("Connecting to REDIS server at %s" % self._resource['redis_host'])
            pilot_service = pilot.PilotComputeService(redis_url)

            # Launch Pilot Job
            self._log.append("Launching Pilot Job: %s" % str(pilot_description))
            self._pilotjob = pilot_service.create_pilot(pilot_description)

        except Exception, ex:
            # something went wrong. append the exception to the log 
            # and call the callbacks.
            self._log.append(str(ex))
            self._set_and_propagate_state_change_priv(new_state=FAILED)

    # ------------------------------------------------------------------------
    #
    def _set_and_propagate_state_change_priv(self, new_state):
        """Propagate a state change to all callback functions.
        """
        # do nothing if existing and new state are identical
        if self._state == new_state:
            return

        for callback in self._cbs:
            callback(self, self._state, new_state)
        self._state = new_state


