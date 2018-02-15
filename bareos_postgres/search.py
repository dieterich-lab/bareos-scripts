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
from common.checks         import Checks
checks = Checks() 
_delim = checks.directory_deliminator()
obfpwd = checks.obfuscate_key # Setting as an method object
from common.loghandler import log
from inspect import stack

import atexit
import datetime
import inspect
import ntpath
import os


class Search():
    def __init__(self, parser = {}, *args, **kwargs):
#         self._set_config(parser, args, kwargs) # NEVER REMOVE
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler()# ConfigHandler disabled until py3 update        # Convert parsed args to dict and add to kwargs
        if isinstance(parser, ArgumentParser):
            ### SET ARGPARSE OPTIONS HERE #####################################
            ### ALWAYS SET DEFAULTS THROUGH AN @property ######################
            parser.add_argument('--out',     '-o', action="store", dest="output",         type=str, help='FULL PATH to output filename')
            parser.add_argument('--user','-U',     action="store", dest="user",           type=str, help='Bareos Database Password')
            parser.add_argument('--password','-P', action="store", dest="password",       type=str, help='Bareos Database Password')
            parser.add_argument('--logfile',     '-L', action="store", dest="logfile",    type=str, help='Logfile file name or full path.\nDEFAULT: ./classname.log')
            parser.add_argument('--loglevel',   '-l', action="store", dest="loglevel",    type=str, help='Logging level.\nDEFAULT: 10.')
            parser.add_argument('--screendump',  '-S', action="store", dest="screendump", type=str,  help='For logging only. If "True" all logging info will also be dumped to the terminal.\nDEFAULT: True.')
            parser.add_argument('--createpaths','-C', action="store", dest="createpaths", type=str, help='For logging only. If "True" will create all paths and files (example create a non-existent logfile.\nDEFAULT: True')

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
        log.debug("Starting  {C}...".format(C = self.app_name), 
                 app_name     = self.app_name,
                 logfile      = self.logfile, 
                 log_level    = self.log_level, 
                 screendump   = self.screendump, 
                 create_paths = self.create_paths, 
                 )
        # Start params here
            ### ALWAYS SET DEFAULTS THROUGH AN @property ######################
        self.user       = kwargs.get("user", None)
        self.password   = kwargs.get("password", None)
        self.output     = kwargs.get("output", None)
        
        atexit.register(self._cleanup)
    
        self.main()

    def _cleanup(self):
        try: 
            log.info(self.app_name + " complete.")
            self.FH.close()
        except: 
            pass
        
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
        _value = _dir + _file 
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
        if value is None: value = 10
        try: self.LOGLEVEL = int(value)
        except (ValueError, TypeError):
            _value = str(value).upper()
            if   "CRIT"   in _value: self.LOGLEVEL = 50
            elif "ERR"    in _value: self.LOGLEVEL = 40
            elif "WARN"   in _value: self.LOGLEVEL = 30
            elif "INF"    in _value: self.LOGLEVEL = 20
            elif "D"      in _value: self.LOGLEVEL = 10
            elif "N"      in _value: self.LOGLEVEL = 0
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
        if value is None: value = None
        if value:   self.SCREENDUMP = True
        else:       self.SCREENDUMP = False
                    
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
        if value is None: value = True
        if value:   self.CREATEPATHS = True
        else:       self.CREATEPATHS = False
                    
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
            self.OUTFILE = "/beegfs/scratch/bareos-restores/bareos.file_locations.tmp"
            return self.OUTFILE         

    @outfile.setter
    def outfile(self, value):
        if value is None: self.OUTFILE = "/beegfs/scratch/bareos-restores/bareos.file_locations.tmp"
        self.OUTFILE = str(value)
                    
    @outfile.deleter
    def outfile(self):
        del self.OUTFIL            

    def main(self):
        """"""
        conn = Connect(password  = self.password)
        paths = conn.meta.tables['path']
        FH = open(self.outfile, "w")
        FH.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")
        
        nullnames = 0
        for file in conn.ENGINE.execute("SELECT jobid, pathid, name FROM file"):
            pathid = file[1]
            jobid  = file[0]
            name   = file[2]
        
            if len(name) < 1:
                nullnames += 1
                continue
            
            else:
                if nullnames != 0: log.info("Skipped {N} null filenames.".format(N = str(nullnames)))
                nullnames = 0
        
            path   = conn.ENGINE.execute("SELECT path FROM path WHERE pathid = {P}".format(P = str(pathid))).fetchone()[0]
            line = ''.join([str(jobid), ":", str(path) + str(name)])
        #     print(line)
            try: 
                FH.write(line + "\n")
            except:
#             except UnicodeEncodeError as e:
                line = line.encode('utf-8')
                FH.write(str(line) + "\n")
            
        FH.close()
        
if __name__ == '__main__':
    parser = ArgumentParser()
    object = Search(parser)


