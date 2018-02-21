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
from bareos_postgres.ABC import Bareos_postgres_ABC
from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
obfpwd = checks.obfuscate_key # Setting as an method object
from common.loghandler import log
from inspect import stack

import inspect
import ntpath
import os
import sqlalchemy
        

class Connect(Bareos_postgres_ABC):
    """
    :NAME:
        connect()
        
    :DESCRIPTION:
        This class generates a connection object for the postgres database.   
         
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
        A postgres "Engine" (connection) object. 
        
    :DEVELOPER_NOTES:
        This class uses Sqlalchemy, so should be easily modded to handle other
        databases.
         
    """
    def __init__(self, parser = {}, *args, **kwargs):
        # Always set the defaults via the @property
        super().__init__(parser, *args, **kwargs)
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(self.user, self.password, self.host, self.port, self.database)
        # The return value of create_engine() is our connection object
        try: self.engine = sqlalchemy.create_engine(url, client_encoding='utf8')
        except Exception as e:
            err = "Unable to create Postgres engine with  user = '{U}', password = '{P}',  host= '{H}', port = '{O}', database = '{D}'".format( U = self.user, 
                                                                                                                                                P = obfpwd(self.password), 
                                                                                                                                                H = self.host,
                                                                                                                                                O = self.port,
                                                                                                                                                D = self.database
                                                                                                                                                )
            log.error(err)
            raise RuntimeError(err)

        try: self.meta =sqlalchemy.MetaData(bind=self.engine, reflect=True)
        except Exception as e:
            err = "Unable to create Postgres Meta object from SQLAlchemy engine '{E}'".format( E = str(self.engine))
            log.error(err)
            raise RuntimeError(err)
            
    def execute(self, *args, **kwargs):
        self.ENGINE(args, kwargs)