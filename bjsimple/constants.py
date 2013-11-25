#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

# ----------------------------------------------------------------------------
# MULTIPROCESSING POOL DATA TRANSFER WORKERS
MAX_WORKERS     = 4 # DO NOT CHANGE! 

# ----------------------------------------------------------------------------
# UPDATE INTERVAL OF THE THREAD MAIN LOOP
UPDATE_INTERVAL = 1

# ----------------------------------------------------------------------------
# OTHER CONSTANTS
DEFAULT         = 'Default'

# ----------------------------------------------------------------------------
# STATE CONSTANTS
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
