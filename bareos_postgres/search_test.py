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
# files = conn.meta.tables['file']
paths = conn.meta.tables['path']
FH = open("./bareos.file_locations.tmp", "w")

nullnames = 0
for file in conn.ENGINE.execute("SELECT jobid, pathid, name FROM file"):
    jobid  = file[0]
    pathid = file[1]
    name   = file[2]

    if len(name) < 1:
        nullnames += 1
#         print("!!! name is null!!! '{F}'".format(F = str(name)))
        continue
    
    else:
        if nullnames != 0: print("Skipped {N} null filenames.".format(N = str(nullnames)))
        nullnames = 0

    path   = conn.ENGINE.execute("SELECT path FROM path WHERE pathid = {P}".format(P = str(pathid))).fetchone()[0]
    line = ''.join([str(jobid), ":", str(path) + str(name)])
#     print(line)
    FH.write(line + "\n")
    
FH.close()
