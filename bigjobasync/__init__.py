#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

from task import * 
from constants import * 
from resource import Resource
from resource_dictionary import RESOURCES

import os
version=open(os.path.dirname (os.path.abspath (__file__)) + "/VERSION", 'r').read().strip()
