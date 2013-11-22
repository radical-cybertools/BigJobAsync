import uuid

# ----------------------------------------------------------------------------
# CONSTANTS
COPY            = 'Copy'
LOCAL_FILE      = 'LocalFile'
REMOTE_FILE     = 'RemoteFile'

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

        self._cb = []
        self._name = name
        self._executable = executable
        self._arguments = arguments
        self._input = input
        self._output = output

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

        self._cb.extend(callbacks)

    # ------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid

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
    @property
    def input_transfers(self):
        """Returns the list of input files transfers.
        """
        return self._name

    # ------------------------------------------------------------------------
    #
    @property
    def output_transfers(self):
        """Returns the list output files to transfer.
        """
        return self._name