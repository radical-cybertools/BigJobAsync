import uuid

# ----------------------------------------------------------------------------
# CONSTANTS
COPY            = 'Copy'
LOCAL           = 'LocalFile'
REMOTE          = 'RemoteFile'

NEW             = "New"
PENDING         = "Pending"
TRANSFER_INPUT  = "TransferInput"
RUNNING         = "Running"
TRANSFER_OUTPUT = "TransferOutput"
FAILED          = "Failed"
DONE            = "Done"

# ----------------------------------------------------------------------------
#
class Task(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, name, executable, arguments, input=[], output=[]):
        """Constructs a new Task object.
        """
        self._uid = uuid.uuid4()

        self._cbs = []
        self._log = []
        self._state = NEW

        self._name = name
        self._executable = executable
        self._arguments = arguments
        self._input = input
        self._output = output

        self._dir_name = "%s__%s" % (self.name, self.uid)

    # ------------------------------------------------------------------------
    #
    def register_callbacks(self, callbacks):
        """Registers one or more new callback function(s). 

        Callbacks are called whenever a Task object makes a state transition.
        Possible states are:

            * PENDING
            * TRANSFER-INPUT
            * RUNNING
            * TRANSFER-OUTPUT
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
    def _set_and_propagate_state_change_priv(self, new_state):
        """Propagate a state change to all callback functions.
        """
        # do nothing if existing and new state are identical
        if self._state == new_state:
            return

        for callback in self._cbs:
            callback(self, self._state, new_state)
        self._state = new_state

