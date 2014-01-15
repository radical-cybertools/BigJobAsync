#!/usr/bin/env python

"""The task representation / interface in BigJobAsync.
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import uuid
from constants import * 
from tracing   import Traceable

# ----------------------------------------------------------------------------
#
class Task(Traceable):

    # ------------------------------------------------------------------------
    #
    def __init__(self, name, executable, arguments=[], environment={}, 
                 input=[], output=[], cores=1):
        """Constructs a new Task object.
        """
        self._uid = uuid.uuid4()

        self._cbs = []
        self._log = []
        self._state = NEW

        self._name        = name
        self._executable  = executable
        self._arguments   = arguments
        self._environment = environment
        self._cores       = cores
        self._input       = input
        self._output      = output

        self._dir_name = "%s__%s" % (self.name, self.uid)
        self._remote_workdir_url = None

        # Traceable interface
        Traceable.__init__(self)

    # ------------------------------------------------------------------------
    #
    def register_callbacks(self, callbacks):
        """Registers one or more new callback function(s). 

        Callbacks are called whenever a Task object makes a state transition.
        Possible states are:

            * PENDING
            * TRANSFER_INPUT
            * RUNNING
            * DONE_WAITING_FOR_TRANSFER
            * TRANSFER_OUTPUT
            * DONE 
            * FAILED
        """
        if not isinstance(callbacks, list):
            callbacks = [callbacks] 

        self._cbs.extend(callbacks)

    # ------------------------------------------------------------------------
    #
    def __str__(self):
        """String representation. Returns the task name.
        """
        return self.name

    # ------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid

    # ------------------------------------------------------------------------
    #
    @property 
    def state(self):
        return self._state

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
    def dir_name(self):
        return self._dir_name


    # ------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the Task.
        """
        return self._name

    # ------------------------------------------------------------------------
    #
    @property
    def cores(self):
        """Returns the number of cores required by the Task.
        """
        return self._cores

    # ------------------------------------------------------------------------
    #
    @property
    def executable(self):
        """Returns the executable.
        """
        return self._executable    

    # ------------------------------------------------------------------------
    #
    @property
    def arguments(self):
        """Returns the executable arguments.
        """
        return self._arguments
    
    # ------------------------------------------------------------------------
    #
    @property
    def environment(self):
        """Returns the executable environment.
        """
        return self._environment
    

    # ------------------------------------------------------------------------
    #
    @property
    def output(self):
        """Returns the output file directives.
        """
        return self._output

   # ------------------------------------------------------------------------
    #
    @property
    def input(self):
        """Returns the input file directives.
        """
        return self._input

    # ------------------------------------------------------------------------
    #
    def _set_state(self, new_state):
        """Propagate a state change to all callback functions.
        """
        # do nothing if existing and new state are identical
        if self._state == new_state:
            return

        old_state = self._state
        self._state = new_state

        for callback in self._cbs:
            callback(self, old_state, new_state)

