#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__      = "Mike Rightmire"
__copyright__   = "Universitäts Klinikum Heidelberg, Section of Bioinformatics and Systems Cardiology"
__license__     = "Not licensed for private use."
__version__     = "0.9.0.0"
__maintainer__  = "Mike Rightmire"
__email__       = "Michael.Rightmire@uni-heidelberg.de"
__status__      = "Development"

from bareos_postgres import Connect

from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
from common.loghandler import log
from inspect import stack

# import inspect
# import ntpath
# import os
import time

# filegrep = input("File grep string: ")

conn = Connect(password  = "in4bareospostgres")
files = conn.meta.tables['file']

for f in conn.ENGINE.execute(files.select()): 
    print(f)