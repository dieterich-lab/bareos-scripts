#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__      = "Mike Rightmire"
__copyright__   = "Universitäts Klinikum Heidelberg, Section of Bioinformatics and Systems Cardiology"
__license__     = "Not licensed for private use."
__version__     = "0.9.0.0"
__maintainer__  = "Mike Rightmire"
__email__       = "Michael.Rightmire@uni-heidelberg.de"
__status__      = "Development"

from argparse       import ArgumentParser
from bareos_postgres import Connect
from bareos_postgres.ABC import Bareos_postgres_ABC

from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
obfpwd = checks.obfuscate_key # Setting as an method object
from common.loghandler import log
from inspect import stack
from stat import S_IREAD, S_IRGRP, S_IROTH

import atexit
import datetime
import inspect
import ntpath
import os


class Search(Bareos_postgres_ABC):
    """
    :NAME:
        search()
        
    :DESCRIPTION:
        This class searches the Bareos Postgres database for files that have 
        been backed up. 
        
        THIS IS MOSTLY A PLACEHOLDER FOR NOW: 
        
        Currently running the search.py will only dump the backed up files out
        as an inventory file. 
        
        Eventually, this script needs to be modified to include a "--grep"
        string which will search the database and return the results.
        
        A switch "--dump" should also be added to allow for the entire filebase 
        to be dumped to the output file.  
         
    :METHODS:
        None
        
    :ATTRIBUTES:
        host       : The postgres host. 
                     (DEFAULT: localhost)

        port        : The postgres connection port. 
                      (DEFAULT: 5432)

        database    : Name of the postgres database. 
                      (DEFAULT: bareos)

        user        : The postgres username
                      (DEFAULT: bareospostgresro)

        password    : Mandatory. Password for the postgres user. 
                      (DEFAULT: None)

        outfile     : The FULL PATH to the output file. 
                      (DEFAULT: /beegfs/prj/bareos_iventory/inventory.csv)
        
        logfile     : (For logfile only). The FULL PATH to the logfile. 
                      (DEFAULT: ./<scriptname.log>)
        
        log_level   : (For logfile only). Set logging level (same as 
                      python.logger module..) 
                      (DEFAULT: 10 ['DEBUG'])
        
        screendump  : (For logfile only). If True, than all logging is ALSO 
                      dumped to STDOUT.
                      (DEFAULT: True)
                              
        create_paths: (For logfile only). If directory paths do not exist, 
                      automatically create them. (DEFAULT: True)
                      (DEFAULT: True)
        
    :RETURNS:
        None
        
    :DEVELOPER_NOTES:
        Currently running the search.py will only dump the backed up files out
        as an inventory file. 
        
        Eventually, this script needs to be modified to include a "--grep"
        string which will search the database and return the results.
        
        A switch "--dump" should also be added to allow for the entire filebase 
        to be dumped to the output file.  
    
    """
    def __init__(self, parser = {}, *args, **kwargs):
#         self._set_config(parser, args, kwargs) # NEVER REMOVE
        self.app_name = self.__class__.__name__
        super().__init__(parser, *args, **kwargs)

        atexit.register(self._cleanup)
    
        self.main()

    def _cleanup(self):
        try: 
            log.info(self.app_name + " complete.")
            self.FH.close()
            os.chmod(self.outfile, S_IREAD|S_IRGRP|S_IROTH)
        except: 
            pass
        
    def main(self):
        """"""
        conn = Connect(*self.args, **self.kwargs)
        paths = conn.meta.tables['path']
        log.info("Writing to: '{F}'".format(F = str(self.outfile)))
        FH = open(self.outfile, "w")
#         FH.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")
        
        nullnames = 0
        for file in conn.ENGINE.execute("SELECT jobid, pathid, name FROM file"):
            pathid = file[1]
            jobid  = file[0]
            name   = file[2]
        
            if len(name) < 1:
                nullnames += 1
                continue
            else:
                if nullnames > 0: log.info("Skipped {N} null filenames.".format(N = str(nullnames)))
                nullnames = 0
        
            path     = conn.ENGINE.execute("SELECT path FROM path WHERE pathid = {P}".format(P = str(pathid))).fetchone()[0]
            _startend = conn.ENGINE.execute("SELECT starttime, realendtime FROM job WHERE jobid = {J}".format(J = str(jobid))).fetchone()
            _start = _startend[0].strftime('%Y-%m-%d') 
#             _end   = _startend[1].strftime('%Y-%m-%d')
#             startend = ''.join([_start, "-", _end])
            line = ''.join([str(jobid), ":", _start, ":", str(path) + str(name)])
#             print(line) #333
            try: 
                FH.write(line + "\n")
            except UnicodeEncodeError as e:
                line = line.encode('utf-8')
                FH.write(str(line) + "\n")
            
        FH.close()
        
if __name__ == '__main__':
    parser = ArgumentParser()
    object = Search(parser)


