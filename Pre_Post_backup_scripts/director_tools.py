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
from pathlib import Path
from shutil import copyfile
import tempfile

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
  Reschedule Interval = 1 minute
  Reschedule On Error = yes
  Reschedule Times = 5
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
  Exclude Dir Containing = .nobackup
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
Job {{
  Name = "{NAME}"
  FileSet = "{FILESET}"
  JobDefs = "{JOBDEF}"
}}
"""
# .format(
#     NAME = "NAME", 
#     FILESET = "FILESET", 
#     JOBDEF = "JOBDEF" 
# )
### Factories ###############################################################

def _name_from_path(dir):
    # "/the/full/path" to "Autogen-the_full_path"     
    return ''.join(["Autogen-", str(dir).replace(_delim, "_").strip("_")])

    
class ABC_bareos(metaclass=abc.ABCMeta):
    """
    :NAME:
        ABC_bareos (Abstract class)
        
    :DESCRIPTION:
        ABC_bareos is an abstract class for scripts used by the Bareos backup
        tool. It sets the common properties and methods expected to be common 
        for most if not all scripts. 
        
        IF A NEW SCRIPT DEFINES A PROPERTY OR METHOD that will be useful to 
        many or all Bareos scripting tools, PLEASE PUT IT HERE.
        
    :PROPERTIES:
        directory:  The directory upon which the script will be enacted. When 
                    a direcctory is a mandatory property, failure to provide 
                    the parameter at instantiation will raise an error.
                    
                    E.g. To generate "jobdefs" files, a directorry is parsed,
                    and a "jobdefs" file is created for each top-level 
                    sub-directoy.
                    DEFAULT. (No default)   
                    
        director_path: The path in which lives the Bareos Director's 
                       configuration files.                        
                       DEFAULT: /etc/bareos/bareos-dir.d 
        
        dirgid:    The numeric Group ID (GID) for the Bareos Director user. 
                   DEFAULT: (Obtained from "bareos" group in /etc/group)
        
        
        diruid:    The numeric User ID (UID) for the Bareos Director user. 
                   DEFAULT: (Obtained from "bareos" user in /etc/passwd)

        logfile:   The FULL PATH to the logfile. 
                   DEFAULT: "./<subclass_name>.log"
        
        log_level: The log level based on the Python "logging" package. 
                   DEFAULT: 10
                   
        screendump: If "True", all logging lines are also sent to STDOUT.
                    DEFAULT: True
                    
        create_paths: (Logging only) If a path (such as to the logfile) does not
                      exits, automaticall create it
                      DEFAULT: True
                      
        symlinks:    When creating Baroe files (specifically the fileset), if 
                     "symlinks" is True, create the file to follow symlinks. 
                     E.g. "/dir1/dir2/." (with period) include symlinks.
                          "/dir1/dir2/" (WITHOUT period) do not include symlinks
        
    :METHODS:
        controlled_delete: Allows a controlled process when deleting 
                           automatically created files (such as filesets or 
                           jobdefs). 
                           
                           This method can be over-ridden as needed within 
                           child-classes to obtain the desireed delete results. 
                           
                           DEFAULT BEHAVIOR: Actually move the file being
                           deleted to /tmp/ for later permanent removal. 
            
    """
    def __init__(self, parser = {}, *args, **kwargs):
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler()# ConfigHandler disabled until py3 update
        # Convert parsed args to dict and add to kwargs
        if isinstance(parser, ArgumentParser):
#             parser = self._arg_parser(parser)
            ### ALWAYS SET DEFAULTS IN @property ##################################
            parser.add_argument('--directory', action="store", dest="DIRECTORY", type=str, default = None, help='The directory upon which action is based. (I.e. --generate --jobdefs --directory /gen/jobdef/files/FOR/this/dir')
            parser.add_argument('--director-path', action="store", dest="DIRECTORPATH", type=str, default = None, help='The directory upon which action is based. (I.e. --generate --jobdefs --directory /gen/jobdef/files/FOR/this/dir')
            parser.add_argument('--full', action='store_true', dest="FULL", help='Run a Full backup  ')
            parser.add_argument('--logfile', '-L', action="store", dest="LOGFILE", type=str, default = None, help='Logfile file name or full path.\nDEFAULT: ./classname.log')
            parser.add_argument('--log-level', '-l', action="store", dest="LOGLEVEL", type=str, default = None, help='Logging level.\nDEFAULT: 10.')
            parser.add_argument('--screendump', '-S', action="store", dest="SCREENDUMP", type=str, default = None, help='For logging only. If "True" all logging info will also be dumped to the terminal.\nDEFAULT: True.')
            parser.add_argument('--create-paths', '-C', action="store", dest="CREATEPATHS", type=str, default = None, help='For logging only. If "True" will create all paths and files (example create a non-existent logfile.\nDEFAULT: True')
            parser.add_argument('--test', action='store_true', dest="TEST", help='"test" mode only. Do not perform any real actions (I.e. file writes). \nDEFAULT: False')
            parser.add_argument('--dir-uid', action="store", dest="DIRUID", type=str, default = None, help='The user ID or username of for the bareos install. \nDEFAULT: bareos')
            parser.add_argument('--dir-gid', action="store", dest="DIRGID", type=str,  default = None, help='The group ID or groupname of for the bareos install. \nDEFAULT: bareos')
            parser.add_argument('--include-symlinks', action='store_true', dest="SYMLINKS", help='Backup the actual data from symlinks. This is the equiv of putting a "." at the end of the fileset path. I.e. "/root/dir/."')
            ###################################################################
            
            parser_kwargs = parser.parse_args()
            kwargs.update(vars(parser_kwargs))

        elif isinstance(parser, dict):
            kwargs.update(parser)
            
        else:
            err = "{C}.{M}: Parameter 'parser' ({P}) must be either an Argparse parser object or a dictionary. ".format(C = self.app_name, M = inspect.stack()[0][3], P = str(parser))
            raise ValueError(err)
        
        # Set classwide
        self.parser = parser
        self.args   = args
        self.kwargs = kwargs 

        # # Here we parse out any args and kwargs that are not needed within the self or self.CONF objects
        # # if "flag" in args: self.flag = something
        ### ALWAYS SET DEFAULTS IN @property #################################
        # # Logging
        self.logfile        = kwargs.get("LOGFILE",     None)
        self.log_level      = kwargs.get("LOGLEVEL",    None)
        self.screendump     = kwargs.get("SCREENDUMP",  None)
        self.create_paths   = kwargs.get("CREATEPATHS", None)
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
        #ONLY the params local to this class. Subcleasses do their own
        self.directory      = kwargs.get("DIRECTORY",   None)
        self.director_path  = kwargs.get("DIRECTORPATH",None)
        self.diruid         = kwargs.get("DIRUID",      None)
        self.dirgid         = kwargs.get("DIRGID",      None)
        self.symlinks       = kwargs.get("SYMLINKS",    None)
        self.test           = True if kwargs.get("TEST", False) else False
        

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
    def directory(self):
        try: return self.DIRECTORY
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
         
    @directory.setter
    def directory(self, value):
        if value is None: 
            err = "Parameter '{A}' has been set to 'None'. This may cause an error for certain operations.".format(A = str(stack()[0][3]))
            log.warning(err)
            self.DIRECTORY = value
            return
#             raise ValueError(err)
         
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
    def director_path(self):
        try: return self.DIRECTORPATH
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
         
    @director_path.setter
    def director_path(self, value):
        if value is None: value = "/etc/bareos/bareos-dir.d" 
        _value = str(value)
        _value = _value if _value.endswith(_delim) else _value + _delim
        # Do checks and such here
        if (not os.path.isdir(_value)):
            err = "Attribute '{A}. '{V}' does not appear to exist or is not readable.".format(A = str(stack()[0][3]), V = _value)
            log.error(err)
            raise ValueError(err)
        else:
            self.DIRECTORPATH = _value
 
    @director_path.deleter
    def director_path(self):
        del self.DIRECTORPATH
    
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
            log.warning(err)
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
            log.warning(err)
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
        _dir    = ntpath.dirname(_value) + _delim
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
    def symlinks(self):
        try: return self.SYMLINKS
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.error(err)
            raise ValueError(err)
        
    @symlinks.setter
    def symlinks(self, value):
        if value is None: value = ""
        if value        : self.SYMLINKS = "." # Adds a dot
        else            : self.SYMLINKS = "" # Adds nothing

    @symlinks.deleter
    def symlinks(self):
        del self.SYMLINKS

    def controlled_delete(self, p):
        _p = str(p)
        msg = "DELETING FILE: '{P}'".format(P = _p)
        if self.test: msg  += "(test only)" 
        log.warning(msg)

        filename = ntpath.basename(_p)
        lastdir  = ntpath.dirname(_p)
        lastdir  = lastdir.split(_delim)
        lastdir  = lastdir[len(lastdir)-1]
        tmpdir   = os.path.join(tempfile.gettempdir(), "bareos_director_deletes", lastdir)

        if not os.path.isdir(tmpdir):
            try: os.makedirs(tmpdir)
            except Exception as e:
                err = "Unable to create 'deletion' directory '{D}'. Aborting delete. Please remove files manually. (ERR: {E}".format(D = tmpdir, E = str(e))
                log.error(err)
                raise RuntimeError(err)

        dst = os.path.join(tmpdir, filename)
            
#         print("Copy from '{P}' to '{D}'".format(P = _p, D = dst) )
        msg = "copyfile({P}, {D})' ...".format(P = str(p), D = dst)
        if self.test: msg += "OK (test only)"
        else:
            try: 
                copyfile(str(p), dst)
                msg += "OK"
                log.debug(msg)        
            except Exception as e:
                msg += "FAILED! (ERROR: {E})".format(E=str(e))
                log.error(msg)        
                
        msg = "{P}.unlink ...".format(P = str(p))
        if self.test: msg += "OK (test only)"
        else:
            try: 
                p.unlink()
                msg += "OK"
                log.debug(msg)        
            except Exception as e:
                msg += "FAILED! (ERROR: {E})".format(E=str(e))
                log.error(msg)        


class DirectorTools(ABC_bareos):
    """
    :NAME:
        DirectorTools
        
    :DESCRIPTION:
        DirectorTools is an all-encompassing script for manipulating Bareos
        with specific tasks. 
        
    :PROPERTIES:
    
    :PUBLIC METHODS:
    
    :PRIVATE METHODS:
                           
        gen_filesets:    (Generate filesets) Creates the "filesets" config files 
                         for the directory set in the "directory" property.
                         
                          
        rem_filesets:    (Remove filesets) Deletes the "filesets" config files 
                         for the directory set in the "directory" property.
                         Uses the "controlled_delete" method. 
                          
        gen_jobs:        (Generate jobs) Creates the "job" config files 
                         for the directory set in the "directory" property.
        
        rem_jobs:        (Remove jobs) Deletes the "job" config files 
                         for the directory set in the "directory" property.
                         Uses the "controlled_delete" method.
        
        gen_jobdefs:    (Generate jobdefs) Creates the "jobdefs" config files 
                         for the directory set in the "directory" property.
        
        rem_jobdefs:    (Remove jobdefs) Deletes the "jobdefs" config files 
                         for the directory set in the "directory" property.
                         Uses the "controlled_delete" method.    
    """
    def __init__(self, parser = {}, *args, **kwargs):        
        # Always set the defaults via the @property
        if isinstance(parser, ArgumentParser):
            parser.add_argument('--generate', action='store_true', dest="GENERATE", help='Create the necessary files with in director. Must be accompanied by an appropriate task type (I.e. --generate --jobdefs --directory /gen/jobdef/files/FOR/this/dir).')
            parser.add_argument('--remove', action='store_true', dest="REMOVE", help='Remove created files with in director. Must be accompanied by an appropriate task type (I.e. --remove --jobdefs --directory /remove/jobdef/files/FOR/this/dir).')
            parser.add_argument('--type', action='store', dest="TYPE", type=str, default = None, help='Type of job (Backup, Archive, etc. DEFAULT: Backup')
            parser.add_argument('--level', action='store', dest="LEVEL", type=str, default = None, help='Type of job (Full, Incremental, Differential. DEFAULT: Full')
            parser.add_argument('--pool', action='store', dest="POOL", type=str, default = None, help='Which storage pool to use. DEFAULT: <Same as level>')
            parser.add_argument('--client', action='store', dest="CLIENT", type=str, default = None, help='The backup client. DEFAULT: phobos-fd')
            parser.add_argument('--schedule', action='store', dest="SCHEDULE", type=str, default = None, help='Set the scheduling (for jobdefs only)')
            parser.add_argument('--storage', action='store', dest="STORAGE", type=str, default = None, help='Set the storage medium. DEFAULT: Tape')
            parser.add_argument('--messages', action='store', dest="MESSAGES", type=str, default = None, help='Messages setting for jobdefs.')
            parser.add_argument('--priority', action='store', dest="PRIORITY", type=int, default = None, help='Priority setting for jobdefs.')
            parser.add_argument('--bootstrap', action='store', dest="BOOTSTRAP", type=str, default = None,help='bootstrap for jobdefs. DEFAULT: "/var/lib/bareos/%c.bsr".')
            parser.add_argument('--fullbackuppool', action='store', dest="FULLBACKUPPOOL", type=str, default = None, help='The generic "Full" backup pool.')
            parser.add_argument('--diffbackuppool', action='store', dest="DIFFBACKUPPOOL", type=str, default = None, help='The generic "Differential" backup pool.')
            parser.add_argument('--incbackuppool', action='store', dest="INCBACKUPPOOL", type=str, default = None, help='The generic "Incremental" backup pool.')
            
        super().__init__(parser, args, kwargs)

        # Always set the defaults via the @property
        self.backup_type    = self.kwargs.get("TYPE",            None) 
        self.level          = self.kwargs.get("LEVEL",           None) 
        self.pool           = self.kwargs.get("POOL",            None) 
        self.client         = self.kwargs.get("CLIENT",          None) 
        self.schedule       = self.kwargs.get("SCHEDULE",        None) 
        self.storage        = self.kwargs.get("STORAGE",         None) 
        self.messages       = self.kwargs.get("MESSAGES",        None) 
        self.priority       = self.kwargs.get("PRIORITY",        None) 
        self.bootstrap      = self.kwargs.get("BOOTSTRAP",       None) 
        self.fullbackuppool = self.kwargs.get("FULLBACKUPPOOL",  None) 
        self.diffbackuppool = self.kwargs.get("DIFFBACKUPPOOL",  None) 
        self.incbackuppool  = self.kwargs.get("INCBACKUPPOOL",   None) 
        self.generate       = self.kwargs.get("GENERATE",        False) 
        self.remove         = self.kwargs.get("REMOVE",          False) 
        
        self.main()
                
#===============================================================================
#     @property
#     def jobdefs(self):
#         try: return self.JOBDEFS
#         except (AttributeError, KeyError, ValueError) as e:
#             err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.info(err)
# #             raise ValueError(err)
#             return False
#         
#     @jobdefs.setter
#     def jobdefs(self, value):
#         if value is None: value = None
#         if value:   self.JOBDEFS = True
#         else:       self.JOBDEFS = False
#                     
#     @jobdefs.deleter
#     def jobdefs(self):
#         del self.JOBDEFS
#     
#     @property
#     def filesets(self):
#         try: return self.FILESETS
#         except (AttributeError, KeyError, ValueError) as e:
#             err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.info(err)
# #             raise ValueError(err)
#             return False
#         
#     @filesets.setter
#     def filesets(self, value):
#         if value is None: value = None
#         if value:   self.FILESETS = True
#         else:       self.FILESETS = False
#                     
#     @filesets.deleter
#     def filesets(self):
#         del self.FILESETS
#===============================================================================

    @property
    def backup_type(self):
        try: return self.TYPE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @backup_type.setter
    def backup_type(self, value):
        if value is None: value = "Backup"
        _value = str(value).upper().strip()
        if   value.startswith("B"):   self.TYPE = "Backup"
        elif value.startswith("A"):   self.TYPE = "Archive"
        else:
            err = "The value for attribute {A} does not appear to be valid ('{V}'). Valid types are 'Backup' or 'Archive'".format(A = str(stack()[0][3]), V = str(value))
            raise ValueError(err)
        
    @backup_type.deleter
    def backup_type(self):
        del self.TYPE
    
    @property
    def bootstrap(self):
        try: return self.BOOTSTRAP
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @bootstrap.setter
    def bootstrap(self, value):
        if value is None: value = "/var/lib/bareos/%c.bsr"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.BOOTSTRAP = _value
        
    @bootstrap.deleter
    def bootstrap(self):
        del self.BOOTSTRAP

# parser.add_argument('--fullbackuppool', action='store', dest="FULLBACKUPPOOL", type=str, default = "Full", help='The generic "Full" backup pool.')

    @property
    def client(self):
        try: return self.CLIENT
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @client.setter
    def client(self, value):
        if value is None: value = "phobos-fd"
        _value = str(value)
        # For now, accept at face value. Maybe add check with pybareos later. 
        self.CLIENT = _value

    @client.deleter
    def client(self):
        del self.CLIENT

    @property
    def diffbackuppool(self):
        try: return self.DIFFBACKUPPOOL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @diffbackuppool.setter
    def diffbackuppool(self, value):
        if value is None: value = "Weekly-Differential"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.DIFFBACKUPPOOL = _value
        
    @diffbackuppool.deleter
    def diffbackuppool(self):
        del self.DIFFBACKUPPOOL

# parser.add_argument('--incbackuppool', action='store', dest="INCBACKUPPOOL", type=str, default = "Incremental", help='The generic "Incremental" backup pool.')

    @property
    def fullbackuppool(self):
        try: return self.FULLBACKUPPOOL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @fullbackuppool.setter
    def fullbackuppool(self, value):
        if value is None: value = "6mo-Full"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.FULLBACKUPPOOL = _value
        
    @fullbackuppool.deleter
    def fullbackuppool(self):
        del self.FULLBACKUPPOOL

# parser.add_argument('--diffbackuppool', action='store', dest="DIFFBACKUPPOOL", type=str, default = "Differential", help='The generic "Differential" backup pool.')

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
        if value is None: 
            if self.directory is None or self.directory == "":
                err = "The Attribute 'directory' cannot be 'None' when using the 'generate' switch. Please specify the directory from which to generate backup config files. "
                log.error(err)
                raise RuntimeError(err)
            
        if value:   self.GENERATE = True
        else:       self.GENERATE = False
                    
    @generate.deleter
    def generate(self):
        del self.GENERATE
    
    @property
    def incbackuppool(self):
        try: return self.INCBACKUPPOOL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @incbackuppool.setter
    def incbackuppool(self, value):
        if value is None: value = "Daily-Incremental"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.INCBACKUPPOOL = _value
        
    @incbackuppool.deleter
    def incbackuppool(self):
        del self.INCBACKUPPOOL
    
    @property
    def level(self):
        try: return self.LEVEL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @level.setter
    def level(self, value):
        if value is None: value = "Full"
        _value = str(value).upper().strip()
        if   value.startswith("F"):   self.LEVEL = "Full"
        elif value.startswith("D"):   self.LEVEL = "Differential"
        elif value.startswith("I"):   self.LEVEL = "Incremental"
        else:
            err = "The value for attribute {A} does not appear to be valid ('{V}'). Valid types are 'Full', 'Differential', or 'Incremental'".format(A = str(stack()[0][3]), V = str(value))
            raise ValueError(err)
        
    @level.deleter
    def level(self):
        del self.LEVEL

    @property
    def messages(self):
        try: return self.MESSAGES
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @messages.setter
    def messages(self, value):
        if value is None: value = "Standard"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.MESSAGES = _value
        
    @messages.deleter
    def messages(self):
        del self.MESSAGES

# parser.add_argument('--priority', action='store', dest="PRIORITY", type=int, default = 10, help='Priority setting for jobdefs.')

    @property
    def priority(self):
        try: return self.PRIORITY
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @priority.setter
    def priority(self, value):
        if value is None: value = 10
        try: _value = int(value)
        except ValueError as e:
            err = "Cannot set attribute {A} to value '{V}'. Value must be an integer.".format(A = str(stack()[0][3]), V = str(value))
        # Assume OK. Maybe validate later using pybareos or something
        self.PRIORITY = _value
        
    @priority.deleter
    def priority(self):
        del self.PRIORITY

# parser.add_argument('--bootstrap', action='store', dest="BOOTSTRAP", type=str, default = "/var/lib/bareos/%c.bsr",help='bootstrap for jobdefs. DEFAULT: "/var/lib/bareos/%c.bsr".')

    @property
    def pool(self):
        try: return self.POOL
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @pool.setter
    def pool(self, value):
        if value is None:
            try:  value = self.LEVEL
            except AttributeError as e:
                err = "Unable to set determine the value for {A} from either passed in value '{V}' or the parameter '--type'. ".format(A = str(stack()[0][3]), V = str(value))
        _value = str(value)
        # Assume OK since there can be many pools. Maybe validate later using pybareos or something
        self.POOL = _value
        
    @pool.deleter
    def pool(self):
        del self.POOL

# parser.add_argument('--schedule', action='store', dest="SCHEDULE", type=str, default = "", help='Set the scheduling (for jobdefs only)')

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

    @property
    def schedule(self):
        try: return self.SCHEDULE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @schedule.setter
    def schedule(self, value):
        if value is None: value = "RegularBackups"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.SCHEDULE = _value
        
    @schedule.deleter
    def schedule(self):
        del self.SCHEDULE

# parser.add_argument('--storage', action='store', dest="STORAGE", type=str, default = "Tape", help='Set the storage medium. DEFAULT: Tape')

    @property
    def storage(self):
        try: return self.STORAGE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            log.info(err)
            raise ValueError(err)
#             return False
        
    @storage.setter
    def storage(self, value):
        if value is None: value = "Tape"
        _value = str(value)
        # Assume OK. Maybe validate later using pybareos or something
        self.STORAGE = _value
        
    @storage.deleter
    def storage(self):
        del self.STORAGE

# parser.add_argument('--messages', action='store', dest="MESSAGES", type=str, default = None, help='Messages setting for jobdefs.')

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

    def gen_filesets(self):
        """
        DO NOT call directly. Call via DirectorTools
        """
        # only grab one level deep
        for dir in self.dir_list:
            if dir.startswith('.'): 
                log.warning("Skipping hidden file/directory: '{D}'".format(D = dir))
                continue
            # Make fileset
            dir     = dir if dir.endswith(_delim) else dir + _delim
            _name   = _name_from_path(dir)
            _filename = ''.join([self.director_path, "fileset", _delim, _name, ".conf"])
            
            _template = fileset_template.format(
                        NAME  = _name,
                        FILES =''.join(["    File = \"", dir,  self.symlinks, "\""])
                        )
            
            msg = "Creating: '{F}'".format(F = _filename)
            if self.test:  msg += " (TEST ONLY)"
            log.debug(msg)
            if self.test: 
                print(_template)
            else:
                with open(_filename, "w") as FH: 
                    FH.write(_template)
                os.chmod(_filename, 0o755)
                os.chown(_filename, self.diruid, self.dirgid)

    def rem_filesets(self):
        # For now simple and dirty
        _path = ''.join([self.director_path, "fileset", _delim])
        for p in Path(_path).glob("Autogen-*.conf"):
            self.controlled_delete(p)

    def gen_jobs(self):
        """
        DO NOT call directly. Call via DirectorTools
        """
        # only grab one level deep
        for dir in self.dir_list:
            if dir.startswith('.'): 
                log.warning("Skipping hidden file/directory: '{D}'".format(D = dir))
                continue
            # Make fileset
            dir     = dir if dir.endswith(_delim) else dir + _delim
            _name   = _name_from_path(dir)
            _filename = ''.join([self.director_path, "job", _delim, _name, ".conf"])
            
            _template = job_template.format(
                        NAME    = _name,
                        FILESET = _name,
                        JOBDEF  = _name,
                        )
            
            msg = "Creating: '{F}'".format(F = _filename)
            if self.test:  msg += " (TEST ONLY)"
            log.debug(msg)
            if self.test: 
                print(_template)
            else:
                with open(_filename, "w") as FH: 
                    FH.write(_template)
                os.chmod(_filename, 0o755)
                os.chown(_filename, self.diruid, self.dirgid)

    def rem_jobs(self):
        # For now simple and dirty
        _path = ''.join([self.director_path, "job", _delim])
        for p in Path(_path).glob("Autogen-*.conf"):
            self.controlled_delete(p)

    def gen_jobdefs(self):
        """
        DO NOT call directly. Call via DirectorTools
        """
        # only grab one level deep
        for dir in self.dir_list:
            if dir.startswith('.'): 
                log.warning("Skipping hidden file/directory: '{D}'".format(D = dir))
                continue
            dir     = dir if dir.endswith(_delim) else dir + _delim
            _name   = _name_from_path(dir)
            _filename = ''.join([self.director_path, "jobdefs", _delim, _name, ".conf"])
            
            _template = jobdef_template.format(
                        NAME  = _name,
                        TYPE  = self.backup_type,
                        LEVEL = self.level,
                        POOL  = self.pool,
                        CLIENT = self.client, 
                        FILESET = _name, # Uses the fileset name, not the actual dir
                        SCHEDULE = self.schedule,
                        STORAGE = self.storage,
                        MESSAGES = self.messages, 
                        PRIORITY = self.priority,
                        BOOTSTRAP = self.bootstrap, 
                        FULLBACKUPPOOL = self.fullbackuppool,
                        DIFFBACKUPPOOL = self.diffbackuppool, 
                        INCBACKUPPOOL  = self.incbackuppool         
                        )
            
            msg = "Creating: '{F}'".format(F = _filename)
            if self.test:  msg += " (TEST ONLY)"
            log.debug(msg)
            if self.test: 
                print(_template)
            else:
                with open(_filename, "w") as FH: 
                    FH.write(_template)
                os.chmod(_filename, 0o755)
                os.chown(_filename, self.diruid, self.dirgid)

    def rem_jobdefs(self):
        # For now simple and dirty
        _path = ''.join([self.director_path, "jobdefs", _delim])
        for p in Path(_path).glob("Autogen-*.conf"):
            self.controlled_delete(p)
    
    def main(self):
        """"""
        if self.directory:      log.info("directory = '{D}'".format(D = self.DIRECTORY))
        if self.director_path:  log.info("director_path = '{D}'".format(D = self.DIRECTORPATH))
        if self.remove:         log.info("remove = '{D}'".format(D = self.REMOVE))
        if self.generate:       log.info("generate = '{D}'".format(D = self.GENERATE))
        log.info("test = '{D}'".format(D = self.test))

        if self.generate:
            self.dir_list = self.get_dir_list()
            self.gen_jobdefs()
            self.gen_filesets()
            self.gen_jobs()

        elif self.remove: 
            self.rem_jobdefs()
            self.rem_filesets()
            self.rem_jobs()
            
        else:
            err = ''.join(["No main command action called. \n", 
                           "Please use '--generate' to create the job files. \n",
                           "or         '--remove' to delete the job files. \n",
                           ])
            log.error(err)
            raise RuntimeError(err)
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = DirectorTools(parser)
