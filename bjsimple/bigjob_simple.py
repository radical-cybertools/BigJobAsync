import saga
import pilot
# ----------------------------------------------------------------------------
# RESOURCE DICTIONARY
RESOURCES = {
    'XSEDE.STAMPEDE': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'slurm+ssh://login1.stampede.tacc.utexas.edu',
        'jobmgr_queue'  : 'normal',
        'shared_fs_url' : 'sftp://login1.stampede.tacc.utexas.edu/"',
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
    def __init__(self, resource, runtime, cores, project_id, base_dir, queue=DEFAULT):
        """Creates a new BigJob instance.
        """
        self._cbs = []
        self._log = []
        self._tasks = []

        self._resource = resource
        self._runtime = runtime
        self._cores = cores
        self._project_id = project_id
        self._base_dir = base_dir
        self._queue = queue

        self._pilot_job = None
        self._state = None

        self._set_and_propagate_state_change_priv(
            old_state=self._state, 
            new_state=NEW, 
        )

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
        self._launch_pj_priv()

        self._set_and_propagate_state_change_priv(
            old_state=self._state, 
            new_state=PENDING
        )

    # ------------------------------------------------------------------------
    #
    def wait(self):
        """Waits...
        """
        self._set_and_propagate_state_change_priv(
            old_state=self._state, 
            new_state=DONE
        )

    # ------------------------------------------------------------------------
    #
    def schedule_tasks(self, tasks):
        """Schedules one or more tasks for execution.
        """
        if not isinstance(tasks, list):
            tasks = [tasks] 
        self._tasks.extend(tasks)

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
            url.path = self._base_dir
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
            self._set_and_propagate_state_change_priv(
                old_state=self._state, 
                new_state=FAILED
            )


    def _set_and_propagate_state_change_priv(self, old_state, new_state):
        """Propagate a state change to all callback functions.
        """
        for callback in self._cbs:
            callback(self, self._state, new_state)
        self._state = new_state
