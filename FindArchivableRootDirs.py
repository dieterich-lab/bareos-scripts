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
# from common.confighandler  import ConfigHandler # Needs to be updated for Python3
from common.loghandler     import log
from common.convert_timestring_input import convert_timestring_input as get_seconds 

import atexit
import inspect
import os
import re

class Find(object):
    def __init__(self, parser = {}, *args, **kwargs):
        atexit.register(self._cleanup)
        self._set_config(parser, args, kwargs)
        self.main()
                
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
                            action="store", dest="FILETYPE", type=str, choices=set(("d","D","f","F", "b", "B")), default = 'B', 
                            help='Type of file system object to use for collecting dates. \n d=directories (only) \n  f=files (only) \n  b=both. DEFAULT: Both')

        parser.add_argument('--older', '-O', 
                            action="store", dest="OLDER", type=str, default = '1d', 
                            help='Collect statistics on directories/files older than <int><years/months/days/hours/seconds>. DEFAULT: 1 day')

        parser.add_argument('--newer', '-N', 
                            action="store", dest="NEWER", type=str, default = '1d', 
                            help='Collect statistics on directories/files newer than <int><years/months/days/hours/seconds>. DEFAULT: 1 day')

        parser.add_argument('--out', '-o', 
                            action="store", dest="OUTFILE", type=str, default = None, 
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
    
    def _cleanup(self):
        message = "Calling cleanup..."
        try: 
            # All cleanup here ##################################
            if self.outfile is not None: self._outfile.close()
            #####################################################
            message += "OK"
            log.info(message)
        except Exception as e:
            message += "FAILED ({E})".format(E = str(e))
            log.error(message)
        
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
        self.start = kwargs.pop("STARTDIR", None) # Let defaults be set by argparse
        self.older = kwargs.pop("OLDER", None) # Let defaults be set by argparse
        self.newer = kwargs.pop("NEWER", None) # Let defaults be set by argparse
        self.outfile = kwargs.pop("OUTFILE", None) # Let defaults be set by argparse
        self.MAXDEPTH = kwargs.pop("MAXDEPTH", None) # Let defaults be set by argparse
        
        # Everything else goes into the conf
        #==== # confighandler Needs to be updated for Python3 ==================
        # for key, value in kwargs.iteritems():
        #     self.CONF.set(key, value)
        #=======================================================================
                
        # Log something
        log.debug("Running {C}.{M}...".format(C = self.app_name, M = inspect.stack()[0][3]), 
                 app_name     = self.app_name, 
                 logfile      = self.logfile, 
                 log_level    = self.log_level, 
                 screendump   = self.screendump, 
                 create_paths = self.create_paths, 
                 )

    @property
    def startdir(self):
        try:
            return self.STARTDIR # Just the path name
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @startdir.setter
    def startdir(self, value):
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
        
    @startdir.deleter
    def startdir(self):
        del self.STARTDIR

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
    def outfile(self):
        try:
            return self.OUTFILE # Just the path name
        except (AttributeError, KeyError, NameError) as e:
            err = "Attribute {A} is not set. ".format(A = str(inspect.stack()[0][3]))
            raise AttributeError(err)
        
    @outfile.setter
    def outfile(self, value):
        if value is None: 
            self.OUTFILE = None
            return 
        
        try:
            self._outfile = open(value, "w+")
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
            self.OLDER = get_seconds(value, 's')
        except ValueError as e:
            err = "Failed to set attribute '{P}' with '{V}'. ({E})".format(P = str(inspect.stack()[0][3]), V = str(value), E = str(e))
            raise ValueError(err)
        
    @older.deleter
    def older(self):
        del self.OLDER
                        
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
            self.NEWER = get_seconds(value, "s")
        except ValueError as e:
            err = "Failed to set attribute '{P}' with '{V}'. ({E})".format(P = str(inspect.stack()[0][3]), V = str(value), E = str(e))
            raise ValueError(err)
        
    @newer.deleter
    def newer(self):
        del self.NEWER

    def walkit(self):
        self.WALK = {}
        for root, dirs, files in os.walk(".", topdown=True):
            

        
    def main(self):
        """"""
        log.debug("Running 'Find' with parameters: {D}".format(D = str(self.__dict__)))
        log.debug("Done.")
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = Find(parser)
    print(object.older)
