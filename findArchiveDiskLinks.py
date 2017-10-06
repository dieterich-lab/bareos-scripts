#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==================================
# USE PYTHON 3 SYNTAX WHERE POSSIBLE
# ==================================

__author__      = "Mike Rightmire"
__copyright__   = "Universitäts Klinikum Heidelberg, Section of Bioinformatics and Systems Cardiology"
__license__     = "Not licensed for private use."
__version__     = "0.9.0.0"
__maintainer__  = "Mike Rightmire"
__email__       = "Michael.Rightmire@uni-heidelberg.de"
__status__      = "Development"


from argparse       import ArgumentParser
# from common.confighandler  import ConfigHandler # Disabled until updated to py3
# from loghandler     import log # Disabled until bug-fixed
from common.checks         import Checks
from common.directory_tools import findLinks

checks = Checks()
_delim = checks.directory_deliminator()

from inspect import stack
# import inspect
import os

class FindArchiveDiskLinks(object):
    def __init__(self, parser = {}, *args, **kwargs):
        self._set_config(parser, args, kwargs)
        self.main()

    @property
    def start_dir(self):
        try:
            return self.START_DIR
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        
    @start_dir.setter
    def start_dir(self, value):
        _value = str(value)
        # Must be full path, so min is /
        if ( (value is None) or (len(_value) < 1) or (not _value.startswith(_delim)) ): 
            self.START_DIR = None
            return
        # Add end slash if not included
        if not _value.endswith(_delim): _value += _delim        
        # Check path
        if not os.path.isdir(_value):
            err = "The value passed in for attribute {A} ({V}) does not appear to be an existing directory.".format(A = str(stack()[0][3]), V = _value)
#             log.error(err)# loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        else:
            self.START_DIR = _value
    
    @start_dir.deleter
    def start_dir(self):
        del self.START_DIR
                
    @property
    def search_dirs(self):
        try:
            return self.SEARCH_DIRS
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        
    @search_dirs.setter
    def search_dirs(self, value):
        """"""
        # Must be a list
        if isinstance(value, str):
            import ast
            value = ast.literal_eval(value)

        err = "Attribute '{A}' must be a list, containing directory FULL PATHS as strings. (Value={V})"
        if not isinstance(value, (list,tuple)):
            raise ValueError(err.format(A = str(stack()[0][3]), V = str(value)))
        # remove and duplicates
        value = list(set(value))
        # Check values
        for dir in value:
            if not isinstance(dir, str) or not value.startswith(_delim): # Must be full path
                raise ValueError(err.format(A = str(stack()[0][3]), V = str(value)))
            # Add end slash if needed
            elif not dir.endswith(_delim):
                r = value.index(dir) # Get the first instance
                value[r] = dir + _delim
        
        self.SEARCH_DIRS = value
                    
    @search_dirs.deleter
    def search_dirs(self):
        del self.SEARCH_DIRS
                
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
        parser.add_argument('--directory', '-d', action="store", dest="START_DIR", type=str, default = None, help='Starting directory for search.')
        parser.add_argument('--search_dirs', '-D', action="store", dest="SEARCH_DIRS", action='append', nargs='*', default = ["/beegfs", "phobos:/mnt/group_cd"], help='Directories in which to search for missing files. ')
        return parser
    
    def _set_config(self, parser, args, kwargs):
        """"""
        # Set class-wide
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler() # Disabled until updated to py3
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
        #=======================================================================
        # # Logging
        # self.logfile    = kwargs.pop('log_leveL', 'system') # Default warning
        # self.log_level  = kwargs.pop('log_leveL', 10) # Default warning
        # self.screendump = kwargs.pop('screendump', True) # Default off
        # self.formatter  = kwargs.pop('formatter', '%(asctime)s-%(name)s-%(levelname)s-%(message)s')
        # self.create_paths = kwargs.pop('create_paths', True) # Automatically create missing paths
        #=======================================================================
        # parser stuff
        self.start_dir = kwargs.pop("START_DIR", None)
        self.search_dirs = kwargs.pop("SEARCH_DIRS", None)
        self._yes_exists = {}
        self._no_exists = {}
        # Everything else goes into the conf
        for key, value in kwargs.items():
            self.CONF.set(key, value)
        
        #=== LOGGING DISABLED FOR NOW, DUE TO WEIRD PROBLEMS WITH THE SYSTEM ===
        # # Log something
        # log.debug("Running {C}.{M}...".format(C = self.app_name, M = inspect.stack()[0][3]), 
        #          app_name     = self.app_name,
        #          logfile      = self.logfile, 
        #          log_level    = self.log_level, 
        #          screendump   = self.screendump, 
        #          create_paths = self.create_paths, 
        #          )
        #=======================================================================
    
    
        
    #===========================================================================
    # def findLinks(self):
    #     print("self.start_dir=", self.start_dir) #333
    #     for name in os.listdir(self.start_dir):
    #         if name not in (os.curdir, os.pardir):
    #             full = os.path.join(self.start_dir, name)
    #             if os.path.islink(full):
    #                 yield full, os.readlink(full)
    #===========================================================================

    def exists(self, link):
        """
        Must return a dict
        """
        path = str(link)
        
        if not path.startswith(_delim): path = _delim + path
        # If it exists, gather info
        result = {"link":path}
        
        if os.path.exists(path):
            result["exists"] = True
            result["stat"] = os.stat(path)
        
            if os.path.isdir(path) :  
                result["type"] = "dir"
                _subdirs = next(os.walk(path))[1]
                _files = next(os.walk(path))[2]
                result["directories"] = len(_subdirs)
                result["files"] = len(_files)
                
            elif os.path.isfile(path):  
                result["type"] = "file"
            
            else:
                result["type"] = "unknown"
                return result

        else:
            result["exists"] = False
        
        return result
        
    def score_found_by_path(self):
        """
        Scores the found file against original path
        """
        pass
        
    def search(self, path):
        """
        Search the directoris in seach_dirs for the filename. 
        The try to score it against the original file
        """
        #  path must be a full path, strip to name
        all_found = {} # results of all finds
        for search_path in self-search_dirs:
            pass
            # Use subprocess to do a find for filename in each search dir
            # Add founds (full path) to all_found
            # Score the found path against the original path to see if they are similar. 
            # Add all_found[original path] = {found_path:score}
        
            
    def main(self):
        """"""
        # Results is a dict, key = the original link path
        self.results = {}
        while self.start_dir is None:
            # Assume command line call without parameter
            try:
                self.start_dir = input("Enter the FULL PATH of the directory to search:")
                if self.start_dir is None: raise AttributeError("'None' is not valid. ")
            except (ValueError,AttributeError) as e:
                err = "Invalid input ({E}). \nTry again.".format(E = str(e))
                print(err)
            
        for dir, link in findLinks(self.start_dir):
#         for dir, link in findLinks(self.start_dir, use = "os"):
            print("dir:", dir)
            print("Link:", link)
            result = self.exists(link)
            print(result)
            if result["exists"]:
                self._yes_exists[dir] = result
            else:
                self._no_exists[dir] = result
        
        # Now we have all the links as existing or not. 
        # Next we need to confirm if the 'existing' links make sense
#         self.exists_sanity_check() # Will add impossibly 'exists' to the no_exists    
        # Search for the missing files. 
                
        print("YES======")
        print(self._yes_exists)
        print()
        print("NO--------")
        print(self._no_exists)
#         log.debug("Done.")
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = FindArchiveDiskLinks(parser)
