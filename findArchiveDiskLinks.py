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
checks = Checks()
_delim = checks.directory_deliminator()
from common.directory_tools import findLinks
from common.runsubprocess import RunSubprocess as run

from inspect import stack
# import inspect
import atexit
# Put an exist dump of all searcch information and critical dicts
import ntpath
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
            if not isinstance(dir, str) or not dir.startswith(_delim): # Must be full path
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
        parser.add_argument('--search_dirs', '-D', action="append", dest="SEARCH_DIRS", nargs='*', default = ["/beegfs", "/mnt/group_cd"], help='Directories in which to search for missing files. ')
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
        self._no_exists  = {}
        self.searches    = {}
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
        
    def score_found_by_path(self, orig, found):
        """
        Scores the found file against original path
        MUST RETURN A DICT {found:<int>}
        Go backwards, 0 for a match, -1 for a mismatch
        
        A match of [0,0,0,0,0] is exact, so we can assume it's the right file
        
        Matches like [-1, 0, 0, 0, 0] COULD be the right file, since it could
        be something like...
        /Users/mikes/Documents/tmp/test.c
        versus
        /archive/lab/Users/mikes/Documents/tmp/test.c
        
        The longer the match FROM the RIGHT, the more likely it's correct. 
        [-1, 0, 0, 0, 0, 0, 0, 0]
        much more likely than 
        [-1, 0]
        
        A match like [0,-1,0,0] (a mismatch in the middle) is probably NOT the 
        right file, since this is likely a dramatic difference like...
        /Users/mikes/Documents/tmp/test.c
        versus
        /Users/tim/Documents/tmp/test.c
        """
        # Score
        # Super simple matching_dirs/total/dirs
        # be sure to ignore empty list item (sometimes extra directory delims are reported)
        orig_l  = orig.split(_delim)
        found_l = found.split(_delim)
        score_l = []
        # Go backwards, 0 for a match, -1 for a mismatch
        count = len(orig_l) if len(orig_l) > len(found_l) else len(found_l)
        for i in range(1, count, 1):
            try:    orig_w = orig_l[len(orig_l)-i]
            except: orig_w = ""
            try:    found_w = found_l[len(found_l)-i]
            except: found_w = ""
            if orig_w == found_w: score_l = [0]  + score_l
            else                : score_l = [-1] + score_l
        print(found, ":", score_l) #333
        autoset = False
        if sum(score_l) == 0:
            autoset = True
            print("Pretending to make the link here...")
        return {found:[score_l, {"automatically linked": autoset}]}
        
    def search(self, path):
        """
        Search the directoris in seach_dirs for the filename. 
        The try to score it against the original file
        """
        #  path must be a full path, strip to name
        _path = str(path)
        if not _path.startswith(_delim): 
            err = "{C}.{M}: Path '{P}' does not seem to be a full path.".format(C = self.__class__.__name__, M = stack()[0][3], P = _path)
#             raise ValueError(err)
            print(err)
            return [] 
        
        if _path.endswith(_delim):
            err = "{C}.{M}: Path '{P}' does not seem to end in a filename.".format(C = self.__class__.__name__, M = stack()[0][3], P = _path)
#             raise ValueError(err)
            print(err)
            return [] 
        
        _dir  = ntpath.dirname(_path)
        _file = ntpath.basename(_path)
        all_found    = [] # results of all finds
        scored_found = {}
        for search_path in self.search_dirs:
            command = ["find", search_path, "-type", "f",  "-name", _file]
            _result = run(command, output = "list")
            result = []
            for path in _result:
                path = _delim + _delim.join(i for i in path.split(_delim) if len(i) > 0)
                result.append(path)
            # Sometimes extraneous directory delimiters sneak in. Remove
            
            all_found += result 
            # Use subprocess to do a find for filename in each search dir
            # Add founds (full path) to all_found
            # Score the found path against the original path to see if they are similar. 
            # Add all_found[original path] = {found_path:score}
        for found in all_found:
            result = self.score_found_by_path(_path, found)
            scored_found.update(result)
            # Try/except defacto checks for first addition of a list
            try:
                if self.searches[_path]: # list item already exists
                     self.searches[_path].append(result)
            except KeyError as e:
                self.searches[_path] = [result]
                    
    def main(self):
        """"""
#         self.search_dirs = ["/mnt/group_cd"]
        ########################################################
        ### Kluge. This should be input not hard coded, fix #333 
        self.search_dirs = ["/mnt/group_cd", "/beegfs/prj"]
        ########################################################
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
            
        for dir, link in findLinks(self.start_dir, use = "os"):
            result = self.exists(link)
            if result["exists"]:
                print("The link: {L} does exist. Moving on".format(L = link)) 
                self._yes_exists[dir] = result
            else:
                print()
                print("=================")
                print ("link: {L} does not exist. Searching...".format(L = link))
                self.search(link)
        
        import pickle
        import time
        with open('/home/mrightmire/tmp/findArchiveDiskLinks-' + str(time.time()) + ".out", 'wb') as f:
            pickle.dump(self.searches, f)
        
#===============================================================================
#         for key, value in self.searches.items():
#             print("======================")
#             print(key)
#             for i in value:
# #                 if "True" in str(i):
#                 print("...", i)   
#===============================================================================
                                
                #self._no_exists[dir]  = result
        # Now we have all the links as existing or not. 
        # Next we need to confirm if the 'existing' links make sense
#         self.exists_sanity_check() # Will add impossibly 'exists' to the no_exists    
        # Search for the missing files. 
#         log.debug("Done.")
        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = FindArchiveDiskLinks(parser)
