#!/usr/bin/env python

"""The BigJobAsync package provides an asynchronous wrapper around 
BigJob, SAGA-Pilot and SAGA. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
from task                import * 
from constants           import * 
from resource            import Resource
from resource_dictionary import RESOURCES

# ------------------------------------------------------------------------------
#
import os
version=open(os.path.dirname (os.path.abspath (__file__)) + "/VERSION", 'r').read().strip()


# ------------------------------------------------------------------------------
#
USE_SAGA_PILOT = False
