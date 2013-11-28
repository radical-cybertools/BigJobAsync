#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

# ----------------------------------------------------------------------------
# MULTIPROCESSING SETTINGS
MAX_INPUT_TRANSFER_WORKERS  = 4
MAX_OUTPUT_TRANSFER_WORKERS = 4

# ----------------------------------------------------------------------------
# UPDATE INTERVAL OF THE THREAD MAIN LOOP
UPDATE_INTERVAL           = 1

# ----------------------------------------------------------------------------
# OTHER CONSTANTS
COPY                      = 'Copy'
LOCAL                     = 'LocalFile'
REMOTE                    = 'RemoteFile'
DEFAULT                   = 'Default'

# ----------------------------------------------------------------------------
# STATE CONSTANTS


NEW                         = "New"
PENDING                     = "Pending"
RUNNING                     = "Running"
WAITING_FOR_EXECUTION       = "WaitingForExecution"
WAITING_FOR_INPUT_TRANSFER  = "WaitingForInputTransfer"
WAITING_FOR_OUTPUT_TRANSFER = "WaitingForOutputTransfer"
TRANSFERRING_OUTPUT         = "TransferringOutput"
TRANSFERRING_INPUT          = "TransferringInput"

FAILED                      = "Failed"
DONE                        = "Done"
