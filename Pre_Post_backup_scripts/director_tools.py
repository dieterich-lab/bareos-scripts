#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from posix import listdir
__author__      = "Mike Rightmire"
__copyright__   = "Universitäts Klinikum Heidelberg, Section of Bioinformatics and Systems Cardiology"
__license__     = "Not licensed for private use."
__version__     = "0.9.0.0"
__maintainer__  = "Mike Rightmire"
__email__       = "Michael.Rightmire@uni-heidelberg.de"
__status__      = "Development"


from argparse       import ArgumentParser
# ConfigHandler disabled until py3 update
# from common.confighandler  import ConfigHandler 
# loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
# from common.loghandler     import log 
from common.checks         import Checks
checks = Checks()
_delim = checks.directory_deliminator()
from common.loghandler import log
from inspect import stack

import abc
import grp
import inspect
import ntpath
import os
import pwd

### FILE TEMPLATES ##########################################################
jobdef_template = """
JobDefs {{
  Name = "{NAME}"
  Type = {TYPE} 
  Level = {LEVEL}
  Pool = {POOL}
  Client = {CLIENT}
  FileSet = "{FILESET}"
  # Schedule = "{SCHEDULE}"
  Storage = {STORAGE}
  Messages = {MESSAGES}
  Priority = {PRIORITY}
  Write Bootstrap = "{BOOTSTRAP}"
  Full Backup Pool = {FULLBACKUPPOOL}
  Differential Backup Pool = {DIFFBACKUPPOOL} 
  Incremental Backup Pool = {INCBACKUPPOOL} 
}}
"""
# .format(
# NAME  = "NAME,
# TYPE  = "TYPE",
# LEVEL = "LEVEL",
# POOL  = "POOL",
# CLIENT = "CLIENT", 
# FILESET = "FILESET",
# SCHEDULE = "SCHEDULE",
# STORAGE = "STORAGE",
# MESSAGES = "MESSAGES", 
# PRIORITY = PRIORITY # NOT A STRING
# BOOTSTRAP = "BOOTSTRAP" 
# FULLBACKUPPOOL = "FULLBACKUPPOOL",
# DIFFBACKUPPOOL = "DIFFBACKUPPOOL", 
# INCBACKUPPOOL  = "INCBACKUPPOOL"         
# )

fileset_template = """
FileSet {{
  Name = "{NAME}"
  Include {{
        Options {{
                CheckFileChanges = yes
                NoATime = yes
                Signature = SHA1
                Verify = 1
        }}
  {FILES}  
  }}
}}
"""
# .format(
#    NAME="fileset_name", 
#    FILES="""                                                                    
#    File = \"/mnt/group_cd/.\" 
#    File = \"/mnt/someething/.\" 
#    """
#)

job_template = """
Job {
  Name = "{NAME}"
  FileSet = "{FILESET}"
  JobDefs = "{JOBDEF}"
}
"""
# .format(
#     NAME = "NAME", 
#     FILESET = "FILESET", 
#     JOBDEF = "JOBDEF" 
# )
### Factories ###############################################################

class gen_bareos(metaclass=abc.ABCMeta):
    def __init__(self, parser = {}, *args, **kwargs):
        self.parser = parser
        self.args   = args
        self.kwargs = kwargs 
        self._set_config(parser, args, kwargs) # NEVER REMOVE

    def _arg_parser(self, parser):
        """
        :NAME:
        _arg_parser
        
        :DESCRIPTION:
        Put all the argparse set up lines here, for example...
            parser.add_argument('--switch', '-s', 
                                action ="store", 
                                dest   ="variable_name", type=str, default = '.', 
                                help   ='Starting directory for search.'
                                )
        
        :RETURNS:
            Returns the parser object for later use by argparse
            
        """
        ### ALWAYS SET DEFAULTS IN @property ##################################
        parser.add_argument('--directory', action="store", dest="DIRECTORY", type=str, default = False, 
                            help='The directory upon which action is based. (I.e. --generate --jobdefs --directory /gen/jobdef/files/FOR/this/dir')
        parser.add_argument('--jobdefs', action='store_true', dest="JOBDEFS", #type=bool, default = False, 
                            help='Create/Delete jobdefs for directory in director. ')
        parser.add_argument('--generate', action='store_true', dest="GENERATE", #type=bool, default = False, 
                            help='Create the necessary files with in director. Must be accompanied by an appropriate task type (I.e. --generate --jobdefs --directory /gen/jobdef/files/FOR/this/dir).')
        parser.add_argument('--remove', action='store_true', dest="REMOVE", #type=bool, default = False, 
                            help='Remove created files with in director. Must be accompanied by an appropriate task type (I.e. --remove --jobdefs --directory /remove/jobdef/files/FOR/this/dir).')
        parser.add_argument('--logfile', '-L', action="store", dest="LOGFILE", type=str, 
                            help='Logfile file name or full path.\nDEFAULT: ./classname.log')
        parser.add_argument('--log-level', '-l', action="store", dest="LOGLEVEL", type=str, 
                            help='Logging level.\nDEFAULT: 10.')
        parser.add_argument('--screendump', '-S', action="store", dest="SCREENDUMP", type=str,  
                            help='For logging only. If "True" all logging info will also be dumped to the terminal.\nDEFAULT: True.')
        parser.add_argument('--create-paths', '-C', action="store", dest="CREATEPATHS", type=str, 
                            help='For logging only. If "True" will create all paths and files (example create a non-existent logfile.\nDEFAULT: True')
        parser.add_argument('--test', '-t', action="store", dest="TEST", type=str, 
                            help='"test" mode only. Do not perform any real actions (I.e. file writes). \nDEFAULT: False')
        parser.add_argument('--dir-uid', action="store", dest="DIRUID", type=str, 
                            help='The user ID or username of for the bareos install. \nDEFAULT: bareos')
        parser.add_argument('--dir-gid', action="store", dest="DIRGID", type=str, 
                            help='The group ID or groupname of for the bareos install. \nDEFAULT: bareos')

        return parser
    
    def _set_config(self, parser, args, kwargs):
        """"""
        # Set class-wide
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler()# ConfigHandler disabled until py3 update
        self.ARGS   = args
        self.KWARGS = kwargs        
        # Convert parsed args to dict and add to kwargs
        if isinstance(parser, ArgumentParser):
            parser = self._arg_parser(parser)
            parser_kwargs = parser.parse_args()
            kwargs.update(vars(parser_kwargs))

        elif isinstance(parser, dict):
            kwargs.update(parser)
            
        else:
            err = "{C}.{M}: Parameter 'parser' ({P}) must be either an Argparse parser object or a dictionary. ".format(C = self.app_name, M = inspect.stack()[0][3], P = str(parser))
            raise ValueError(err)
        
        # #=== loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
        # # Here we parse out any args and kwargs that are not needed within the self or self.CONF objects
        # # if "flag" in args: self.flag = something
        ### ALWAYS SET DEFAULTS IN @property #################################
        # # Logging
        self.logfile        = kwargs.pop("LOGFILE",     None)
        self.log_level      = kwargs.pop("LOGLEVEL",    None)
        self.screendump     = kwargs.pop("SCREENDUMP",  None)
        self.create_paths   = kwargs.pop("CREATEPATHS", None)
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
        self.directory      = kwargs.pop("DIRECTORY",   None)
        self.generate       = kwargs.pop("GENERATE",    None)
        self.remove         = kwargs.pop("REMOVE",      None)
        self.diruid         = kwargs.pop("DIRUID",      "bareos")
        self.dirgid         = kwargs.pop("DIRGID",      "bareos")
        self.test           = True if kwargs.pop("TEST", False) else False 

    @property
    def directory(self):
        try: return self.DIRECTORY
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
         
    @directory.setter
    def directory(self, value):
        if value is None: 
            err = "There is no default directory. Value cannot be 'None'"
            log.error(err)
            raise ValueError(err)
         
        _value = str(value)
        # Do checks and such here
        if (not os.path.isdir(_value)):
            err = "Attribute '{A}. '{V}' does not appear to exist or is not readable.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.DIRECTORY = _value
 
    @directory.deleter
    def directory(self):
        del self.DIRECTORY

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
        del self.CREATEPATHS
    
    @property
    def diruid(self):
        try: return self.DIRUID
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @diruid.setter
    def diruid(self, value):
        if value is None: 
            value = "bareos"
            err = "Attribute {A} cannot be set to 'None'. Using default value of '{V}'".format(A = str(stack()[0][3]), V = value)
            log.error(err)
        # Do checks and such here
        try:  self.DIRUID = int(value)
        except ValueError as e:
            try: self.DIRUID = pwd.getpwnam(str(value)).pw_uid 
            except KeyError as e:
                err = "Unable to set Director uid from value '{V}'.".format(V = str(value))
                raise ValueError(err)

    @diruid.deleter
    def diruid(self):
        del self.DIRUID

    @property
    def dirgid(self):
        try: return self.DIRGID
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @dirgid.setter
    def dirgid(self, value):
        if value is None: 
            value = "bareos"
            err = "Attribute {A} cannot be set to 'None'. Using default value of '{V}'".format(A = str(stack()[0][3]), V = value)
            log.error(err)
        # Do checks and such here
        try:  self.DIRGID = int(value)
        except ValueError as e:
            try: self.DIRGID = grp.getgrnam(str(value)).gr_gid 
            except KeyError as e:
                err = "Unable to set Director gid from value '{V}'.".format(V = str(value))
                raise ValueError(err)

    @dirgid.deleter
    def dirgid(self):
        del self.DIRGID


class gen_jobdefs(gen_bareos):
    """
    DO NOT call directly. Call via DirectorTools
    """
    def __init__(self, dir_list, *args, **kwargs):
#                 director_path = "/etc/bareos/bareos-dir.d/",
#                 diruid = 112, # Default for bareos on phobos
#                 dirgid = 119, # Default for bareos on phobos 
#                 CLIENT = "phobos-fd",
#                 TYPE    = "Backup",
#                 LEVEL   = "Full", 
#                 POOL    = "Full",
#                 STORAGE = "Tape",
#                 MESSAGES = "Standard",
#                 PRIORITY = 10,
#                 BOOTSTRAP = "/var/lib/bareos/%c.bsr",
#                 FULLBACKUPPOOL  = "Full",
#                 DIFFBACKUPPOOL  = "Differential" ,
#                 INCBACKUPPOOL   = "Incremental", 
#                 test = False
#                 ):
        self.dir_list       = dir_list
        self.director_path  = kwargs.pop("director_path", "/etc/bareos/bareos-dir.d/")
        self.diruid         = kwargs.pop("diruid", 112)
        self.dirgid         = kwargs.pop("dirgid", 119)

        self.TYPE           = kwargs.pop("TYPE",    "Backup")
        self.LEVEL          = kwargs.pop("LEVEL",   "Full")
        self.POOL           = kwargs.pop("POOL",    "Full")
        self.CLIENT         = kwargs.pop("CLIENT",  "phobos-fd")
        self.SCHEDULE       = kwargs.pop("SCHEDULE", None)
        self.STORAGE        = kwargs.pop("STORAGE", "Tape")
        self.MESSAGES       = kwargs.pop("MESSAGES","Standard")
        self.PRIORITY       = kwargs.pop("PRIORITY", 10)
        self.BOOTSTRAP      = kwargs.pop("BOOTSTRAP","/var/lib/bareos/%c.bsr") 
        self.FULLBACKUPPOOL = kwargs.pop("FULLBACKUPPOOL", "Full")
        self.DIFFBACKUPPOOL = kwargs.pop("DIFFBACKUPPOOL", "Differential")
        self.INCBACKUPPOOL  = kwargs.pop("INCBACKUPPOOL" , "Incremental")         
        self.test           = True if kwargs.pop("test", False) else False 
        
        log.debug("Calling: gen_jobdefs with '{D}'".format(D = self.dir_list))
#         super().__init__(parser, args, kwargs)
        self.main()
    
    def main(self):
        # only grab one level deep
        for dir in self.dir_list:
            if dir.startswith('.'): 
                log.warning("Skipping hidden file/directory: '{D}'".format(D = dir))
                continue
            # Make fileset
            _name           = ''.join(["Autogen-", dir])
            jobdef_filename = ''.join([self.director_path, "jobdefs", _delim, _name, ".conf"])
            
            _jobdef = jobdef_template.format(
                        NAME  = _name,
                        TYPE  = self.TYPE,
                        LEVEL = self.LEVEL,
                        POOL  = self.POOL,
                        CLIENT = self.CLIENT, 
                        FILESET = _name, # Uses the fileset name, not the actual dir
                        SCHEDULE = self.SCHEDULE,
                        STORAGE = self.STORAGE,
                        MESSAGES = self.MESSAGES, 
                        PRIORITY = self.PRIORITY,
                        BOOTSTRAP = self.BOOTSTRAP, 
                        FULLBACKUPPOOL = self.FULLBACKUPPOOL,
                        DIFFBACKUPPOOL = self.DIFFBACKUPPOOL, 
                        INCBACKUPPOOL  = self.INCBACKUPPOOL         
                        )
            
            msg = "Creating: '{F}'".format(F = jobdef_filename)
            if self.test:  msg += " (TEST ONLY)"
            log.debug(msg)
            if self.test: 
                print(_jobdef)
            else:
                with open(jobdef_filename, "w") as FH: 
                    FH.write(_fileset)
                os.chmod(jobdef_filename, 0o755)
                os.chown(jobdef_filename, self.diruid, self.dirgid)


class DirectorTools(gen_bareos):
    def __init__(self, parser = {}, *args, **kwargs):
        self.parser = parser
        self.args   = args
        self.kwargs = kwargs
        super().__init__(parser, args, kwargs)
        self.main()
                
    @property
    def jobdefs(self):
        try: return self.JOBDEFS
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
#             raise ValueError(err)
            return False
        
    @jobdefs.setter
    def jobdefs(self, value):
        if value is None: value = None
        if value:   self.JOBDEFS = True
        else:       self.JOBDEFS = False
                    
    @jobdefs.deleter
    def jobdefs(self):
        del self.JOBDEFS

    @property
    def generate(self):
        try: return self.GENERATE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
#             raise ValueError(err)
            return False
        
    @generate.setter
    def generate(self, value):
        if value is None: value = None
        if value:   self.GENERATE = True
        else:       self.GENERATE = False
                    
    @generate.deleter
    def generate(self):
        del self.GENERATE
    
    @property
    def remove(self):
        try: return self.REMOVE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
#             raise ValueError(err)
            return False
        
    @remove.setter
    def remove(self, value):
        if value is None: value = None
        if value:   self.REMOVE = True
        else:       self.REMOVE = False
                    
    @remove.deleter
    def remove(self):
        del self.REMOVE

    def get_dir_list(self):
        dir_list = []
        dirs = os.listdir(self.directory)
        for dir in dirs: 
            full_path = ''.join([self.directory, dir, _delim])
            if dir.startswith('.'): 
                log.warning("Skipping hidden file/directory: '{P}'".format(P = full_path))
                continue
            dir_list.append(full_path)
    
        return dir_list
    
    def main(self):
        """"""
        if self.directory:  log.info("directory = '{D}'".format(D = self.DIRECTORY))
        if self.jobdefs:    log.info("jobdefs = '{D}'".format(D = self.JOBDEFS))
        if self.generate:   log.info("generate = '{D}'".format(D = self.GENERATE))
        if self.remove:     log.info("remove = '{D}'".format(D = self.REMOVE))
        log.info("test = '{D}'".format(D = self.test))
        dir_list = self.get_dir_list()
        gen_jobdefs( dir_list = dir_list, test = self.test)
        # Check oppositions
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = DirectorTools(parser)
