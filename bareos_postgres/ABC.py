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
from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
obfpwd = checks.obfuscate_key # Setting as an method object
from common.loghandler import log
from inspect import stack

import abc
import inspect
import ntpath
import os
import sqlalchemy
import sys


class Bareos_postgres_ABC(metaclass=abc.ABCMeta):
    """
    :NAME:
        Bareos_postgres_ABC()
        
    :DESCRIPTION:
        The abstract class for Python Bareos Postgres tools.
        
        Predominately, this just handles all the @properties for the 
        connecting the Bareos postgres database.   
        
            """
    def __init__(self, parser = {}, *args, **kwargs):
        self.app_name = self.__class__.__name__
        if isinstance(parser, ArgumentParser):
            ### SET ALL ARGPARSE OPTIONS HERE #################################
            ### ALWAYS SET DEFAULTS THROUGH AN @property ######################
            parser.add_argument('--logfile',     '-L', action="store", dest="logfile",    type=str, help='Logfile file name or full path.\nDEFAULT: ./classname.log')
            parser.add_argument('--loglevel',    '-E', action="store", dest="loglevel",   type=str, help='Logging level.\nDEFAULT: 10.')
            parser.add_argument('--screendump',  '-S', action="store", dest="screendump", type=str, help='For logging only. If "True" all logging info will also be dumped to the terminal.\nDEFAULT: True.')
            parser.add_argument('--createpaths', '-C', action="store", dest="createpaths",type=str, help='For logging only. If "True" will create all paths and files (example create a non-existent logfile.\nDEFAULT: True')
            parser.add_argument('--user',        '-U', action="store", dest="user",       type=str, default = None, help="Database User for accessing the Bareos Postgres database. (DEFAULT: 'bareospostgresro')")
            parser.add_argument('--password',    '-W', action="store", dest="password",   type=str, default = None, help="Database User's Password for accessing the Bareos Postgres database. (DEFAULT: 'None')")
            parser.add_argument('--host',        '-H', action="store", dest="host",       type=str, default = None, help="Host for accessing the Bareos Postgres database. (DEFAULT: 'localhost')")
            parser.add_argument('--port',        '-P', action="store", dest="port",       type=str, default = None, help="Port for accessing the Bareos Postgres database. (DEFAULT: '5432')")
            parser.add_argument('--database',    '-D', action="store", dest="database",   type=str, default = None, help="Bareos Postgres database name to which to connect. (DEFAULT: 'bareos')")
            parser.add_argument('--outfile',     '-o', action="store", dest="outfile",     type=str,help='FULL PATH to outfile filename')

            parser_kwargs = parser.parse_args()
            kwargs.update(vars(parser_kwargs))
            
        elif isinstance(parser, dict):
            kwargs.update(parser)
            
        else:
            err = "{C}.{M}: Parameter 'parser' ({P}) must be either an Argparse parser object or a dictionary. ".format(C = self.app_name, M = inspect.stack()[0][3], P = str(parser))
            raise ValueError(err)
        
        # Set classwide here
        self.parser = parser
        self.args   = args
        self.kwargs = kwargs         

        # # Here we parse out any args and kwargs that are not needed within the self or self.CONF objects
        # # if "flag" in args: self.flag = something
        ### ALWAYS SET DEFAULTS IN @property #################################
        # # Logging
        self.logfile        = kwargs.get("logfile", ''.join(["./", self.app_name, ".log"]))
        self.log_level      = kwargs.get("loglevel", 10)
        self.screendump     = kwargs.get("screendump", True)
        self.create_paths   = kwargs.get("createpaths", True)
        #=== loghandler bugfix in Jessie access to self.socket.send(msg)
        # Only use actual filesystem file for log for now
        # Log something
        log.info("Starting  {C}...".format(C = self.app_name), 
                 app_name     = self.app_name,
                 logfile      = self.logfile, 
                 log_level    = self.log_level, 
                 screendump   = self.screendump, 
                 create_paths = self.create_paths, 
                 )
        # Start params here
            ### ALWAYS SET DEFAULTS THROUGH AN @property ######################
        self.user       = kwargs.get("user",      None)
        self.password   = kwargs.get("password" , None)
        self.host       = kwargs.get("host" ,     None)
        self.port       = kwargs.get("port" ,     None)
        self.database   = kwargs.get("database" , None)
        self.outfile    = kwargs.get("outfile", None)
                            
    @property
    def logfile(self):
        try: return self.LOGFILE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @logfile.setter
    def logfile(self, value):
        if value is None: value = ''.join(["./", self.app_name, ".log"])
        _value  = str(value)
        _dir    = ntpath.dirname(_value)
        _file   = ntpath.basename(_value)
        _basefilename, _ext = os.path.splitext(_file)
        # Do checks and such here
        if (_dir == "") or (_dir.startswith(".")): _dir = os.getcwd() + _delim
        _value = os.path.join(_dir,_file) 
#         _value = _dir + _file 
        self.LOGFILE = _value
    
    @logfile.deleter
    def logfile(self):
        del self.LOGFILE

    @property
    def log_level(self):
        try: return self.LOGLEVEL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @log_level.setter
    def log_level(self, value):
        try: self.LOGLEVEL = int(value)
        except (ValueError, TypeError):
            _value = str(value).upper().strip()
            if   _value.startswith("CRIT"): self.LOGLEVEL = 50
            elif _value.startswith("ERR" ): self.LOGLEVEL = 40
            elif _value.startswith("WARN"): self.LOGLEVEL = 30
            elif _value.startswith("INF" ):  self.LOGLEVEL = 20
            elif _value.startswith("D"   ): self.LOGLEVEL = 10
            elif _value.startswith("N"   ): self.LOGLEVEL = 10
            elif _value.startswith("F"   ): self.LOGLEVEL = 0
            elif _value.startswith("Z"   ): self.LOGLEVEL = 0
            else:
                err = "Unable to determine log level value from'{V}'".format(str(value))
                raise ValueError(err)
                    
    @log_level.deleter
    def log_level(self):
        del self.LOGLEVEL

    @property
    def screendump(self):
        try: return self.SCREENDUMP
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @screendump.setter
    def screendump(self, value):
        _value = str(value).upper().strip()
        if   len(_value) < 1:       self.SCREENDUMP = False
        if   _value.startswith("N"): self.SCREENDUMP = False
        elif _value.startswith("F"): self.SCREENDUMP = False
        else:
            self.SCREENDUMP = True
                    
    @screendump.deleter
    def screendump(self):
        del self.SCREENDUMP

    @property
    def create_paths(self):
        try: return self.CREATEPATHS
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @create_paths.setter
    def create_paths(self, value):
        _value = str(value).upper().strip()
        if   len(_value) < 1:       self.CREATEPATHS = False
        if   _value.startswith("N"): self.CREATEPATHS = False
        elif _value.startswith("F"): self.CREATEPATHS = False
        else:
            self.CREATEPATHS = True

    @create_paths.deleter
    def create_paths(self):
        del self.SCREENDUMP
    
        
#===============================================================================
# if __name__ == '__main__':
#     parser = ArgumentParser()
#     object = ClassName(parser)
#===============================================================================

    @property
    def user(self):
        try: return self.USER
        except (AttributeError, KeyError, ValueError) as e:
            self.USER = "postgres"
            return self.USER 
        
    @user.setter
    def user(self, value):
        if value is None: value = "postgres"
        self.USER = str(value)
                    
    @user.deleter
    def user(self):
        del self.USE

    @property
    def password(self):
        try: return self.PWD
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set and cannot be null. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @password.setter
    def password(self, value):
        if value is None:
            err = "Password parameter cannot be blank. (password = '{V}')".format(V = str(value))
        self.PWD = str(value)
                    
    @password.deleter
    def password(self):
        del self.PWD

    @property
    def outfile(self):
        try: return self.OUTFILE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set and cannot be null. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)

    @outfile.setter
    def outfile(self, value):
        if value is None: self.OUTFILE = "/beegfs/prj/bareos_iventory/inventory.csv"
        self.OUTFILE = str(value)
                    
    @outfile.deleter
    def outfile(self):
        del self.OUTFIL            

    @property
    def user(self):
        try:
            return self.USER
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @user.setter
    def user(self, value):
        if value is None: value = "bareospostgresro"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.USER = _value
    
    @user.deleter
    def user(self):
        del self.USER        
        
    @property
    def password(self):
        try:
            return self.PWD
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @password.setter
    def password(self, value):
        if value is None: value = ""
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.PWD = _value
    
    @password.deleter
    def password(self):
        del self.PWD

    @property
    def host(self):
        try:
            return self.HOST
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @host.setter
    def host(self, value):
        if value is None: value = "localhost"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.HOST = _value
    
    @host.deleter
    def host(self):
        del self.HOST

    @property
    def port(self):
        try:
            return self.PORT
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @port.setter
    def port(self, value):
        if value is None: value = "5432"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.PORT = _value
    
    @port.deleter
    def port(self):
        del self.PORT

    @property
    def database(self):
        try:
            return self.DATABASE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @database.setter
    def database(self, value):
        if value is None: value = "bareos"
        _value = str(value)
        # Do checks and such here
        if (not _value):
            err = "Attribute '{A} = {V}' does not appear to be valid.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.DATABASE = _value
    
    @database.deleter
    def database(self):
        del self.DATABASE

    @property        
    def engine(self):
        try: return self.ENGINE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. Try the 'connection' method. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
                
    @engine.setter
    def engine(self, value):
        if isinstance(value, sqlalchemy.engine.base.Engine):
            self.ENGINE = value

        else:
                err = "Value '{V}'does not appear to be a valid 'sqlalchemy.engine.base.Engine' object".format(V = type(value)) 
                log.error(err)
                raise ValueError(err)

    @engine.deleter
    def engine(self):
        del self.ENGINE
            
    @property        
    def meta(self):
        try: return self.META
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. Try the 'connection' method. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)                        

    @meta.setter
    def meta(self, value):
        if isinstance(value, sqlalchemy.sql.schema.MetaData):
            self.META = value
        else:
                err = "Value '{V}'does not appear to be a valid 'sqlalchemy.sql.schema.MetaData' object".format(V = type(value)) 
                log.error(err)
                raise ValueError(err)

    @meta.deleter
    def meta(self):
        del self.META
