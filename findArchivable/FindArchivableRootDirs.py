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
from common.checks  import Checks
checks = Checks()
_delim = checks.directory_deliminator()
# from common.confighandler  import ConfigHandler # Needs to be updated for Python3
# from common.loghandler               import log
from common.convert_timestring_input import convert_timestring_input as get_time 
from common.convert_timestring_input import age 

import atexit
import inspect
import ntpath
import os
import re
import time

class Find(object):
    """
    :NAME:
        FindArchivableRootDirs.find(parser = {}, *args, **kwargs)
        
    :DESCRIPTION:
        Searches through a directory structure starting at a specified location, 
        and reports the last MODIFICATION date of files and subdirectories. 
        
        The output can be dumped to the terminal, written to file, or both.
        
        Reports are stored within the class object as two dictionaries: 
            self.dictionaries
            self.file
            
            in the format:
            {"file/dir_path_string":[<mtime>, <age>, '<age_delimiter>']}
            I.e.
            {"/Users/mikes":[1506674640.0, 418520.73, 'Hour(s)']}
    
    :OUTPUT:        
        Output to screen or file is in the TEXT format (strings, no lists):
            /Users/mikes:[1506674640.0, 418520.73, 'Hour(s)']
        
        Output to file is (currently) restricted to capturing the text format
        of pickling the output dictionary objects self.directories and 
        self.files. 
        
        Placeholders are set for output to CSV, XLS, XLSX, etc...but
        are not yet implemented. 
            
        Output can be restricted to files (only), directories (only) or both. 
        NOTE: When directories(only) are output, the age is based on the 
              YOUNGEST file within the directory NOT the actual directory 
              creation/modification date!
              
        The script can be run at the command line, using switches OR as a class, 
        passing in variables via the *args/**kwargs parameters. 
        
    :USAGE:
        FindArchivableRootDirs.py --root /Users/user/ \
                                  --type f \ 
                                  --out "./out.txt" \
                                  --older 1y \
                                  --increment d
                                  
        OR
        
        obj = Find( root = "/Users/user/",
                    type = "f",
                    out = "./out.txt",
                    older = "1y",
                    increment = "d",
                    ) 
              
    :ATTRIBUTES:
        conf(str): (Future) Full path to the config file. 
                   DEFAULT: "./FindArchivableRootDirs.conf"
                   
        directories: A dictionary object of the output for directories in the
                     format:
                    {"file/dir_path_string":[<mtime>, <age>, '<age_delimiter>']}
                    I.e.
                    {"/Users/mikes":[1506674640.0, 418520.73, 'Hour(s)']}
                     
        files: A dictionary object of the output for files in the format:
                    {"file/dir_path_string":[<mtime>, <age>, '<age_delimiter>']}
                    {"/Users/mikes":[1506674640.0, 418520.73, 'Hour(s)']}
                    I.e.

        increment(str): The time increment for output. CASE SENSITIVE.
                   I.e.
                   Y = Years
                   M = Months
                   d = Days
                   h = Hours
                   m = Minutes
                   s = Seconds
                   H = Human-readable
                   DEFAULT: Seconds

        maxdepth(int): Similar to find's maxdepth. How many directories deep to
                       limit the search. "0" means unlimited (through all 
                       levels). DEFAULT: 0
        
        newer(str):  (Newer than X) Collect statistics on directories/files that 
                     are "newer than" <int><years/months/days/hours/seconds>.
                     "0" means just report the age from today.  
                     DEFAULT: 0

        older(str):  (Older than X) Collect statistics on directories/files that 
                     are "older than" <int><years/months/days/hours/seconds>.
                     "0" means just report the age from today.  
                     DEFAULT: 0

        out(str): Full path to the output file. This file will lget overwwritten
                  at each run. 
                  DEFAULT: "./FindArchivableRootDirs.txt"')

        root(str): Starting directory for search. DEFAULT: "."
        
        screen(bool): Set to "True" to dump output to screen. Otherwise False. 
                    DEFAULT: True
        
        type(char):  Type of file system objects upon which to report dates. 
                     I.e.
                     d=directories (only)
                     f=files (only)
                     b=both 
                     DEFAULT: b

        (logging only)
        logfile(STR): Output file for logging information (not script output). 
                      DEFAULT: "system"

        log_level(int): Logging level for logging information (not script 
                        output). 
                      DEFAULT: 10 (debug)

        screendump(bool): Should logging output be dumped to the STDOUT.', 
                          DEFAULT: True

        formatter(str): The formatting string for lgging outpput. (See logging 
                        module). 

        create_paths(bool): (Logging only). If a logfile does not exist, should
                            it be created (True/False). 
                            DEFAULT: True')
    
    :METHODS:         
        dump([t = None]):
            where... 
                t = The type (files or directories only)
                
            Dumps the report data to the screen. 
            
        main():  Call method 'walkit' verbatim. If output files or screendumps
                 have been specified at the command line, they are intelligently
                 called. 
        
        walkit(): No parameters accepted at the method level.
                  This runs the actual directory walk and creates the output. 
                  Global 'self' parameters are used to generate criteria. 
                  Some attributes can be modified via a normal object set (I.e.
                  Findobject.maxdepth = 2). 
        
                  :returns: A list of lists containing the same data as the text 
                            output.
        
        write([o = None, t = None,  f = "text"]):
            where...
                f = The file format 
                    text (same as screen output)
                    pickle (pickles the dictionary objects.)
                    csv (Not yet implemented) 
                    xls (Not yet implemented) 
                    xlsx (Not yet implemented) 

                t = The type (files or directories only)
               
                o = Specifies the output file name. If None, then the 
                    existing attribute value for outfile is used. 
                    
            Writes the report dictionary objects "directories" and "files" 
            to a file.

    :RETURNS: 
              Two python dictionary objects: "directories" and "files" which 
              contain the report data (formatted as specified above).  

    """
    def __init__(self, parser = {}, *args, **kwargs):
        atexit.register(self._cleanup)
        self._set_config(parser, args, kwargs)
                
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
        parser.add_argument('--increment', '-i', 
                            action="store", dest="INCREMENTOUT", type=str, default = 's', 
                            help='The time increment for output. CASE SENSITIVE.  \n I.e. \n Y = Years \n M = Months \n d = Days \n h = Hours \n m = Minutes \n s = Seconds  \n H = Human-readable.')

        parser.add_argument('--screen', '-s', 
                            action="store", dest="TERMINAL", type=bool, default = True, 
                            help='Set to "True" to dump output to screen. Otherwise False. DEFAULT: True')
        
        parser.add_argument('--conf', '-c', 
                            action="store", dest="CONFIGFILE", type=str, default = '.', 
                            help='Full path to the config file. DEFAULT: "./FindArchivableRootDirs.conf"')

        parser.add_argument('--root', '-r', 
                            action="store", dest="STARTDIR", type=str, default = '.', 
                            help='Starting directory for search. DEFAULT: "."')
        
        parser.add_argument('--maxdepth', '-m', 
                            action="store", dest="MAXDEPTH", type=int, default = '0', 
                            help='How many directories deep to limit the search. "0" means unlimited (fo through deepest levels). DEFAULT: 0')
        
        parser.add_argument('--type', '-t', 
                            action="store", dest="FILETYPE", type=str, choices=set(("d","D","f","F", "b", "B")), default = 'b', 
                            help='Type of file system object to use for collecting dates. \n d=directories (only) \n  f=files (only) \n  b=both. DEFAULT: Both')

        parser.add_argument('--older', '-O', 
                            action="store", dest="OLDER", type=str, default = '0', 
                            help='Collect statistics on directories/files older than <int><years/months/days/hours/seconds>. DEFAULT: 1 day')

        parser.add_argument('--newer', '-N', 
                            action="store", dest="NEWER", type=str, default = '0', 
                            help='Collect statistics on directories/files newer than <int><years/months/days/hours/seconds>. DEFAULT: 1 day')

        parser.add_argument('--out', '-o', 
                            action="store", dest="OUTFILE", type=str, 
                            help='Full path to the output file. DEFAULT: "./FindArchivableRootDirs.txt"')

        parser.add_argument('--logfile', 
                            action="store", dest="logfile", type=str, default = 'system', 
                            help='Logfile for debugging. DEFAULT: "system"')

        parser.add_argument('--log_level', 
                            action="store", dest="log_level", type=str, default = '10', 
                            help='Logging level for debugging. DEFAULT: 10 (debug)')

        parser.add_argument('--screendump', 
                            action="store", dest="screendump", type=bool, default = True, 
                            help='Screendump logging output to STDERR. DEFAULT: True')

        parser.add_argument('--formatter', 
                            action="store", dest="formatter", type=str, default = '0', 
                            help='Format for logging data. (See logging module)')

        parser.add_argument('--create_paths', 
                            action="store", dest="create_paths", type=bool, default = True, 
                            help='For logging (only) create any missing paths. I.e. if logfile does not exist, create it. DEFAULT: True')

        return parser
    
    def _cascade_dir(self, root, mtime):
        """"""
        # Cascading the directories set the YOUNGEST file value as
        # as the value for all preceding dirs
        # Be sure the formatting comes in consistent
        root = root.strip().rstrip(_delim)
        # If the existing value is older, replace
        _age = age(mtime)
        
        try:
            _append = [mtime, get_time(int(_age), self.INCREMENTOUT), self.increment_readable]
            if self.directories[root][0] < mtime:
                 self.directories[root] = _append
#                 self.directories[root][0] = mtime
        # No inital value, set
        except KeyError as e: # Doesnt exist yet
                self.directories[root] = _append
        # Cascade starting at top dir
        cascade_dirs = root.split(_delim)
        cascade = _delim
        for dir in cascade_dirs:
            cascade = os.path.join(cascade, dir)

            try: 
                # cascade (previous dir) is smaller(older) than root, replace value
                if self.directories[cascade][0] < self.directories[root][0]: 
#                      print(cascade, "is OLDER. Changing:", self.directories[cascade], "to:", self.directories[root])
                    _append = [self.directories[root][0], get_time(int(_age), self.INCREMENTOUT), self.increment_readable]
                    self.directories[cascade] = _append 
            # Cascade dir does not yet exist (which will happen if starting from a relative root)
            except KeyError as e:
                self.directories[cascade] = self.directories[root]

    def _cleanup(self):
        message = "Calling cleanup..."
        try: 
            # All cleanup here ##################################
            if self.outfile is not None: self._outfile.close()
            #####################################################
            message += "OK"
            # log.info(message)
        except Exception as e:
            message += "FAILED ({E})".format(E = str(e))
            # log.error(message)
    
    def _set_config(self, parser, args, kwargs):
        """"""
        # Set class-wide
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler() # Needs to be updated for Python3
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
        
        # Here we parse out any args and kwargs that are not needed within the self or self.CONF objects
        # if "flag" in args: self.flag = something
        # Logging
        self.logfile    = kwargs.pop('log_leveL', 'system') # Default warning
        self.log_level  = kwargs.pop('log_leveL', 10) # Default warning
        self.screendump = kwargs.pop('screendump', True) # Default off
        self.formatter  = kwargs.pop('formatter', '%(asctime)s-%(name)s-%(levelname)s-%(message)s')
        self.create_paths = kwargs.pop('create_paths', True) # Automatically create missing paths
        # parser stuff
        self.start = kwargs.pop("STARTDIR", ".") 
        self.older = kwargs.pop("OLDER", 0)
        self.newer = kwargs.pop("NEWER", 0)
        self.outfile = kwargs.pop("OUTFILE", None)
        self.maxdepth = kwargs.pop("MAXDEPTH", 0)
        self.filetype = kwargs.pop("FILETYPE", "b")
        self.TERMINAL = kwargs.pop("TERMINAL", True)
        self.increment = kwargs.pop("INCREMENTOUT", "s")
        self._now = time.time()
        # Everything else goes into the conf
        #==== # confighandler Needs to be updated for Python3 ==================
        # for key, value in kwargs.iteritems():
        #     self.CONF.set(key, value)
        #=======================================================================
        

        #=======================================================================
        # # Log something
        # log.debug("Running {C}.{M}...".format(C = self.app_name, M = inspect.stack()[0][3]), 
        #          app_name     = self.app_name, 
        #          logfile      = self.logfile, 
        #          log_level    = self.log_level, 
        #          screendump   = self.screendump, 
        #          create_paths = self.create_paths, 
        #          )
        #=======================================================================

    @property
    def directories(self):
        try:
            return self.DIRECTORIES
        except (AttributeError, KeyError, ValueError) as e:
            self.DIRECTORIES = {}
            return self.DIRECTORIES
        
    @property
    def files(self):
        try:
            return self.FILES
        except (AttributeError, KeyError, ValueError) as e:
            self.FILES = {}
            return self.FILES

    @property
    def filetype(self):
        try:
            return self.FILETYPE
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @filetype.setter
    def filetype(self, value):
        _value = str(value).lower()
        if _value[:1] in ["d", "f", "b"]:
            self.FILETYPE = _value[:1]
             
        else:
            err = 'The attribute {A} does not appear to be valid. Acceptable values are "d" (directories), "f" (files), "b" (both).'.format(A = inspect.stack()[0][3])
#             log.error(err))
            raise ValueError(err)

    @filetype.deleter
    def filetype(self, value):
        del self.FILETYPE

    @property        
    def increment(self):
        try:
            return self.INCREMENTOUT
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)

    @increment.setter
    def increment(self, value):
        _value = str(value)
        if _value[:1] in ["Y", "M", "m", "D", "d", "H", "h", "S", "s"]:
            self.INCREMENTOUT = _value[:1]
             
        else:
            err = 'The attribute {A} does not appear to be valid. Acceptable values are "Y" (Year), "M" (Month), "D" or (Day), "h" (Hour), "m" (Minute), "s" (Second), "H" (Human readable)'.format(A = inspect.stack()[0][3])
#             log.error(err))
            raise ValueError(err)

    @increment.deleter
    def increment(self):
        del self.INCREMENTOUT

    @property
    def increment_readable(self):
        try: 
            if   self.INCREMENTOUT.lower().startswith("y"): return "Year(s)" 
            elif self.INCREMENTOUT.startswith("M") or self.INCREMENTOUT.lower().startswith("mo"):  return "Month(s)"
            elif self.INCREMENTOUT.lower().startswith("d"): return "Day(s)" 
            elif self.INCREMENTOUT.startswith("h") or self.INCREMENTOUT.lower().startswith("ho"): return "Hour(s)"
            elif self.INCREMENTOUT.lower() == "m"  or self.INCREMENTOUT.lower().startswith("mi"): return "Minute(s)"
            elif self.INCREMENTOUT.lower().startswith("s") or len(self.INCREMENTOUT) < 1: return "Second(s)"
            elif self.INCREMENTOUT.startswith("H") or self.INCREMENTOUT.lower().startswith("hu"): return ""
            else: return "no match"
        except Exception as e: 
            return str(e)
        
    @property
    def maxdepth(self):
        try:
            return self.MAXDEPTH # Just the path name
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @maxdepth.setter
    def maxdepth(self, value):
        try:
            self.MAXDEPTH = int(value)
        except Exception as e:
            err = "'maxdepth' must be an integer (value = {V}).".format(V = str(value))
            raise ValueError(err)
        
    @maxdepth.deleter
    def maxdepth(self):
        del self.MAXDEPTH
        
    @property
    def newer(self):
        try:
            return self.NEWER
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @newer.setter
    def newer(self, value):
        try:
            self.NEWER = get_time(value, "s")
        except ValueError as e:
            err = "Failed to set attribute '{P}' with '{V}'. ({E})".format(P = str(inspect.stack()[0][3]), V = str(value), E = str(e))
            raise ValueError(err)
        
    @newer.deleter
    def newer(self):
        del self.NEWER

    @property
    def outfile(self):
        try:
            return self.OUTFILE # Just the path name
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @outfile.setter
    def outfile(self, value):
        if "none" in str(value).lower():
            self.OUTFILE = None
            self._outfile = None
            return 

        if value.startswith('.'):
            _current = os.getcwd()
            value = value.replace(".", _current, 1)
        
        try:
            self._outfile = open(value, "w")
            self.OUTFILE = value
        except Exception as e:
            err = "Unable to open {F}. ({E})".format(F = str(value), E = str(e))
            raise IOError(err)
        
    @outfile.deleter
    def outfile(self):
        self._outfile.close()
        del self.OUTFILE

    @property
    def older(self):
        try:
            return self.OLDER
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @older.setter
    def older(self, value):
        try:
            self.OLDER = get_time(value, 's')
        except ValueError as e:
            err = "Failed to set attribute '{P}' with '{V}'. ({E})".format(P = str(inspect.stack()[0][3]), V = str(value), E = str(e))
            raise ValueError(err)
        
    @older.deleter
    def older(self):
        del self.OLDER

    @property
    def start(self):
        try:
            return self.STARTDIR # Just the path name
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @start.setter
    def start(self, value):
        # Make full path
        if value.startswith('.'):
            _current = os.getcwd()
            value = value.replace(".", _current, 1)
            
        try:
            _value = str(value)
            # Strip extra delimitors
            _startdir_list = _value.split(_delim)
            _startdir_list = [x for x in _startdir_list if len(x) > 1]
            _startdir      =  _delim + _delim.join(_startdir_list) + _delim 
            
            if os.path.isdir(_startdir): 
                self.STARTDIR = value
            else: 
                raise IOError()
            
        except Exception as e:
            err = "The {A} ({V}) does not appear to exist or cannot be recognized. ".format(A = str(inspect.stack()[0][3]), V = str(value))
            raise ValueError(err)
                
    @start.deleter
    def start(self):
        del self.STARTDIR
    
    def dump(self, t = None):
        """"""
        if t is not None:
            _t = str(t).lower()[:1]
            self.filetype = _t # Repace global var with input "t"

        if (self.filetype == "d") or (self.filetype == "b"):
            print("DIRS:")
            if len(self.directories) > 0:
                for key,value in self.directories.items():
                    # Minus one for directories, to remove empty split at beginning
                    if ( len(key.split(_delim)) - 1 <= self.maxdepth) or (self.maxdepth == 0):
                        print(key + ":" + str(value))
            else:
                print("None")

        if (self.filetype == "f") or (self.filetype == "b"):
            print("FILES:")
            if len(self.files) > 0:
                for key,value in self.files.items():
                    # NO minus one for files, since filename is not
                    # a 'depth'. Empty first item and filename cancel out
                    if ( len(key.split(_delim)) <= self.maxdepth) or (self.maxdepth == 0):        
                        print(key + ":" + str(value))   
            else:
                print("None")
    
    def read(self, f = None):
        """"""
        if (f is None) and (self.outfile is None):
            err = "{C}.{M}: The class attribute 'outfile' is not set, and 'f' (filename) was not passed. ".format(C = self.__class__.__name__, M = inspect.stack()[0][3])
#             log.error(err)
            raise RuntimeError(err)
        # Open the file
        # Dont reset self.outfile, but if self.outfile exists, use it. 
        if f is not None: _filename = str(f)
        else:             _filename = self.outfile
        try:
            fh = open(_filename, "r")
        except Exception as e:
            err = "{C}.{M}: Unknown error opening file for reading. (Err: {E})".format(C = self.__cöass__.__name__, M = inspect.stack()[0][3], E = str(e))
#             log.error(err)
            raise IOError(err)
        # Assume pickle first
        try:
            value1 = pickle.load(f)
            value2 = pickle.load(f)
        except Exception as e:
            err = "File {F} does not appear to be a pickled file.".format(F = _filename)
#             log.info(err)
        
        # Otherwise assume txt and try to read line by line and convert
        # FUTURE (MARKER FOR CHANGES IF XLS OR CSV IS IMPLEMENTED)
        import ast
        try:
            # reset dicts directly (no setters)
            self.DIRECTORIES = {}
            self.FILES = {}
            _reading = None
            # LOOP THOUGH
            for line in fh:
                if ("DIRS:" in line): 
                    _reading = "d"
                    continue

                if ("FILES:" in line): 
                    _reading = "f"
                    continue
                # Here we have a text line. Parse
                lineitems = line.split(":") # Should only ever be one 
                key = lineitems[0] # already a string. 
                value = ast.literal_eval(lineitems[1])
                if   _reading == "d": self.DIRECTORIES[key] = value
                elif _reading == "f": self.FILES[key] = value
                else:
                    err = "{C}.{M}: There was unexpected content in file '{F}'. Unable to determine if line is a directory or a file item from headers. Did you select that wrong file?".format(C = self.__class__.__name__, M = inspect.stack()[0][3], F = _filename)
#                     log.error(err) 
                    raise RuntimeError(err)
        except Exception as e:
            err = "Unknown error in {C}.{M}: (ERR: {E})".format(C = self.__class__.__name__, M = inspect.stack()[0][3], E = str(e))
#             log.error(err)
            raise type(e)(err)
                 
    def walkit(self):
        """
            walkit(): No parameters accepted at the method level.
                  This runs the actual directory walk and creates the output. 
                  Global 'self' parameters are used to generate criteria. 
                  Some attributes can be modified via a normal object set (I.e.
                  Findobject.maxdepth = 2). 
                  
        :ATTRIBUTES:
            (See main class "Find")
            
        :RETURNS: A list of lists containing the same data as the text output.        
        """
        self.results = []
        
        for root, dirs, files in os.walk(self.start, topdown=True):
            # The current dir age. 
            # Check depth RELATIVE TO root
            # Just set its time for now. The file search with cascade it if needed.
#             self.directories[root] = os.stat(root).st_mtime
            self._cascade_dir(root, os.stat(root).st_mtime)
            dir_depth = root.replace(self.start, "") # Remove relative root
            dir_depth = dir_depth.split(checks.directory_deliminator())
            dir_depth = [x for x in dir_depth if len(x) > 1]
            dir_depth = len(dir_depth) + 1 # Start at 1 not 0
                
#                 _dir_youngest_time = os.stat(root).st_mtime # Reset at each root loop
            # Need to parse files regardless of "-t", since they determine
            # the 'youngest' state of the preceding dirs
            for fn in files:
                path = os.path.join(root, fn)
                try:
                    _time = os.stat(path).st_mtime # in epoch
                except Exception as e:
                    message = "Error gathering mtime from path {P}. Skipping. (ERROR: {E})".format(P = path, E = str(e))
                    # log.error(message)
#                         self.results.append([message])
#                         if self.TERMINAL: print(message)
#                         if self._outfile is not None: self._outfile.write(str([message]) + "\n")
                    self.files[path] = message
                    continue
                
                self._cascade_dir(root, _time)
                
                _diff = self._now - _time
#                     # Set the directory time to the youngest file in the dir
#                     if _time > _dir_youngest_time: 
#                         _dir_youngest_time = _time
                # If it matches the input time range                    
                if ((_diff >= self.older) or (self.older == 0)) and ((_diff <= self.newer) or (self.newer == 0)):
                     # If individual file listings was set
                     if ("f" in self.FILETYPE.lower()) or ("b" in self.FILETYPE.lower()):
                        _append = [_time, get_time(int(_diff), self.INCREMENTOUT), self.increment_readable]
#                              self.results.append(_append)
                        self.files[path] = _append
#                              self.files[path] = _time  
#                              if self.TERMINAL: print(_append)
#                              if self._outfile is not None: self._outfile.write(str(_append) + "\n")
                             
                
        return self.directories, self.files
                
    def write(self, o = None, t = None,  f = "text", m = None):
        """"""
        # Repace global vars with inputs where needed
        if m is not None: self.maxdepth = int(m)
        _f = str(f).lower()
        # Check for pickling first
        if o is not None:
            # First set the outfile. This also set the _outfile
            self.outfile = str(o)
            # Then, if pickle; redo _outfile, dump, and return
            # This leaves the string slef.OUTFILE intact 
            if re.match("^[Pp][IiCcKkLlEe]*$", _f):
                # Close existing outfile if it exists
                try: self._outfile.close()
                except: pass
                # Dump the pickle. No reason to set self._outfile again. 
                try:
                    with open(self.outfile, 'wb') as f:    
                        pickle.dump([self.directories, self.files], f)
                    return
                
                except Exception as e:
                    err = "{C}.{M}: Unknown error trying to create pickle. (ERR: {E})".format(C = self__class__.__name__, M = inspect.stack()[0][3], E = str(e))
        # If not pickle, continue here
        if t is not None: self.filetype =str(t).lower()[:1]
        
        def _raise_not_implemented(t):
            err.format(F = "csv")
#             log.error(err)
            raise NotImplementedError(err)
            
        err = "Output format '{F}' is not yet implemented. "
        if   _f == "csv": _raise_not_implemented("csv") # future
        elif _f == "xls": _raise_not_implemented("xls") # future
        elif _f == "xlsx": _raise_not_implemented("xlsx") # future
        elif _f == "text": 
            try:
                if (self.filetype == "d") or (self.filetype == "b"):
                    self._outfile.write("DIRS:" + "\n")
                    if len(self.directories) > 0:
                        for key,value in self.directories.items():
                            # Minus one for directories, to remove empty split at beginning
                            if ( len(key.split(_delim)) - 1 <= self.maxdepth) or (self.maxdepth == 0):
                                self._outfile.write(key + ":" + str(value) + "\n")
                    else:
                        self._outfile.write("None" + "\n")
        
                if (self.filetype == "f") or (self.filetype == "b"):
                    self._outfile.write("FILES:" + "\n")
                    if len(self.files) > 0:
                        for key,value in self.files.items():
                            # NO minus one for files, since filename is not
                            # a 'depth'. Empty first item and filename cancel out
                            if ( len(key.split(_delim)) <= self.maxdepth) or (self.maxdepth == 0):
                                self._outfile.write(key + ":" + str(value) + "\n")
                    else:
                        self._outfile.write("None" + "\n")
                        
            except AttributeError as e:
                err = "{C}.{M}: Attribute '{A}' has not been set. ({E})".format(C = self.__class__.__name__, M = inspect.stack()[0][3], A = "outfile", E = str(e))
    #             log.error(msg)
                raise AttributeError(err)

    def main(self):
        """
        :NAME:
            main()
                  Calls "walkit()" verbatim.  
                  No parameters accepted at the method level.
                  This runs the actual directory walk and creates the output. 
                  Global 'self' parameters are used to generate criteria. 
                  Some attributes can be modified via a normal object set (I.e.
                  Findobject.maxdepth = 2). 
                  
        :ATTRIBUTES:
            (See main class "Find")
            
        :RETURNS: A list of lists containing the same data as the text output.        
        """
        # log.debug("Running 'Find' with parameters: {D}".format(D = str(self.__dict__)))
        _result = self.walkit()
        if self.TERMINAL: self.dump()
        if self.outfile: self.write()
        # log.debug("Done.")
        return _result
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = Find(parser)
    object.main()
