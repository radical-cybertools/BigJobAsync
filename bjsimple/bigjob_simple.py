#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

# core Python imports
import time
import saga
import pilot
from multiprocessing     import Pool

# framework imports
from task                 import Task 
from constants            import * 
from bigjob_thread        import _BigJobThread
from file_transfer_worker import _file_transfer_worker

# ----------------------------------------------------------------------------
#
def file_transfer_output(origin, old_state, new_state):
    """Big job callback function: writes BigJob state changes to STDERR.

    It aborts the script script with exit code '-1' if BigJob 
    state is 'FAILED'.

    Obviously, more logic can be built into the callback function, for 
    example fault tolerance.
    """ 
    if new_state == DONE_WAITING_FOR_TRANSFER:
        origin._set_and_propagate_state_change_priv(new_state=TRANSFER_OUTPUT)

        print "trasnferring...."

        origin._set_and_propagate_state_change_priv(new_state=DONE)

# ----------------------------------------------------------------------------
#
class BigJobSimple(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, name, resource, runtime, cores, workdir, project_id=None, queue=DEFAULT):
        """Creates a new instance.
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
        self._bj_thread = _BigJobThread(self)


        # The worker pool handles asynchronous interaction with BigJob 
        self._pool = Pool(processes=MAX_WORKERS)  

    # ------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        self._pool.terminate()
        self._pool.join()

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
        self._bj_thread.start()
        

    # ------------------------------------------------------------------------
    #
    def wait(self):
        """Waits...
        """
        self._bj_thread.join()

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


            result = self._pool.apply_async(
                _file_transfer_worker, 
                (self._remote_workdir_url, task))
            results.append(result)

        # r.get() returns when the data-transfer of a task has 
        # completed. Only then we can add them to BigJob. If we wouldn't 
        # synchronize this, jobs might start running before their input 
        # data has been transferred and would subsequently fail. 
        #
        # NOTE: THIS IS NOT OPTIMAL, BUT STILL MUCH BETTER THAN DOING
        #       EVERYTHING SEQUENTIALLY.
        for r in results:
            t = r.get()
            #t.register_callbacks(file_transfer_output)
            self._bj_thread.add_tasks(t)

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
