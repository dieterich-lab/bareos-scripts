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
# from common.confighandler  import ConfigHandler # Disabled until updated to py3
# from loghandler     import log # Disabled until bug-fixed
from common.checks         import Checks
checks = Checks()
_delim = checks.directory_deliminator()
from common.directory_tools import findLinks
from common.runsubprocess import RunSubprocess as run

from inspect import stack
import atexit
import ntpath
import os
import time


class FindArchiveDiskLinks(object):
    def __init__(self, parser = {}, *args, **kwargs):
        self._set_config(parser, args, kwargs)
        atexit.register(self._cleanup)
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
        parser.add_argument('--directory', '-d', action="store", dest="START_DIR", type=str, 
                            default = ".", help='Starting directory for search.')
        parser.add_argument('--search_dirs', '-D', action="store", dest="SEARCH_DIRS", nargs='*', 
                            default = ["/beegfs", "/mnt/group_cd"], help='Directories in which to search for missing files. ')
        parser.add_argument('--output', '-o', action="store", dest="OUTPUT", 
                            default=".", help='Location of the output file. ')
        parser.add_argument('--screendump', '-S', action="store", dest="SCREENDUMP", 
                            default=True, help='Dump output to screen as well as file. ')
        parser.add_argument('--search', '-s', action="store", dest="SEARCH", 
                            default=False, help='Search for the missing link. If True, script searches for the missing link. If False, it simply reports it as missing. ')
        parser.add_argument('--search-hard-drive', action="store", dest="SEARCHHDD", 
                            default=True, help='Search for the missing link. If True, script searches for the missing link. If False, it simply reports it as missing. ')
        parser.add_argument('--only-missing', action="store", dest="ONLYMISSING", 
                            default=True, help='Display only links with a missing linked file. ')
        parser.add_argument('--include-good', action="store", dest="INCLUDEGOOD", 
                            default=False, help='Display only links with a missing linked file. ')
        parser.add_argument('--pickle', action="store", dest="PICKLE", 
                            default=False, help="Python 'Pickle' the search output, instead of text.")
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
        self.start_dir      = kwargs.pop("START_DIR", None)
        self.search_dirs    = kwargs.pop("SEARCH_DIRS", None)
        self.output         = kwargs.pop("OUTPUT", '.')
        self.SCREENDUMP     = kwargs.pop("SCREENDUMP", False)
        if self.SCREENDUMP: self.SCREENDUMP = True # Force to boolean if needed
        self.dosearch       = kwargs.pop("SEARCH", False)
        if self.dosearch: self.dosearch = True 
        self.only_missing   = kwargs.pop("ONLYMISSING", True)
        if self.only_missing: self.only_missing = True 
        _include_good       = kwargs.pop("INCLUDEGOOD", False) # Actually sets self.only_missing
        if _include_good: self.only_missing = False  # Actually sets self.only_missing
        self.PICKLE         = kwargs.pop("PICKLE", False) # Actually sets self.only_missing
        if self.PICKLE: self.PICKLE = True  # Actually sets self.only_missing
        self.SEARCHHDD      = kwargs.pop("SEARCHHDD", True) # Actually sets self.only_missing
        if self.SEARCHHDD: self.SEARCHHDD = True  # Actually sets self.only_missing
        # Script use params
        self._yes_exists = {}
        self._no_exists  = {}
        self.searches    = {}
        # Load all the direcvtory paths from the archive disk listings
        self._archive_listings = []
        try:
            for file in os.listdir("/beegfs/prj/archive_disks"):
                _path = "/beegfs/prj/archive_disks/" + file
                try:
                    self._dump("Adding " + _path )
                    with open(_path, "rb") as fh:
                        for line in fh: 
                            self._archive_listings.append(line)
#                     for line in self._archive_listings: print(line) #333
                except (FileNotFoundError, PermissionError) as e:
                    err = "Unable to open '{P}'. Skipping. ".format(P = _path)
                    self._dump(err)
                    
        except Exception as e:
            err = "Path '/beegfs/prj/archive_disks' does not appear to be available. Skipping file listing and continuing.\n(ERR:{E})".format(E = str(e))
            self._dump(err)
             
        
        #=======================================================================
        # # Everything else goes into the conf
        # for key, value in kwargs.items():
        #     self.CONF.set(key, value)
        #=======================================================================
        
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

    def _cleanup(self):
        exit_msg = "Cleaning up {C}.".format(C = self.__class__.__name__)
        self._dump(exit_msg)
        # Dump the searches dete regardless
        _dir = ntpath.dirname(self.output)
        if self.PICKLE:
            _path = _dir + _delim + "findArchiveDiskLinks-searches" + str(time.time()) + ".pickle"
            msg = ("Dumping searches as pickle...")
            try:
                import pickle
                with open(_path, 'wb') as f:
                    pickle.dump(self.searches, f)
                    msg += "OK"
            except Exception as e:
                msg += "FAILED (ERR:{E})".format(E = str(e))
            self._dump(msg)
                
        else:
            _path = _dir + _delim + "findArchiveDiskLinks-searches" + str(time.time()) + ".text"
            msg = ("Dumping searches as text...")
            try:
                with open(_path, 'w') as f:
                    for key, value in self.searches.items():
                        f.write(key)
                        f.write(value)
                        msg += "OK"
            except Exception as e:
                msg += "FAILED (ERR:{E})".format(E = str(e))
            self._dump(msg)
            
        if self.output:
            try: self.OUPUT_FH.close()
            except Exception as e: pass
        
        print("Finished.")
        
    def _dump(self, *args):
        _args = ' '.join(args)
        """"""
        # Put ny string cleanup here if run into issues
        if self.SCREENDUMP: print(_args)
        if self.OUPUT_FH: self.OUPUT_FH.write(_args + "\n")
            
    @property
    def output(self):
        try:
            return self.OUTPUT
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        
    @output.setter
    def output(self, value):
        if value is None: self.OUTPUT = None
        _value = str(value)
        if _value == ".": _value = os.getcwd() + _delim + "findArchiveDiskLinks" + str(time.time()) + ".out"
        # Must be full path, so min is /
        if ( (value is None) or (len(_value) < 1) or (not _value.startswith(_delim)) ): 
            err = "Attribute '{A}' must be a FULL PATH to a file. (value = '{V}')".format(A = str(stack()[0][3]), V =_value)
            raise ValueError(err)
        # Add end slash if not included
        # Check path
        try:
            self.OUPUT_FH = open(_value, "w")
            self.OUTPUT = _value
        except Exception as e:
            err = "{C}.{A}: Unknown error attempting to open file. (File: '{V}', ERR: '{E}')".format(C = self.__class__.__name__, A = str(stack()[0][3]), V =_value, E = str(e))
            raise IOError(err)
        
    @output.deleter
    def output(self):
        del self.OUTPUT

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
        if (value is None) or (len(_value) < 1) or (_value == '.'): 
            _value = os.getcwd() 
        # Must be full path, so min is /
        if not _value.startswith(_delim):
            err = "The 'directory' parameter must be a FULL PATH, or '.' indicating the current directory. (value ='{V}').".format(V = _value)
            raise ValueError(err)
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
        if not isinstance(value, (list,tuple)) or value is None:
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
        
    def _score_by_path(self, orig, found):
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
        self._dump(str(found) + " : " + str(score_l)) #333
        autoset = False
        if sum(score_l) == 0:
            autoset = True
            self._dump("Pretending to make the link here...")
        return {found:[score_l, {"automatically linked": autoset}]}

    def score_directory_list(self, directories_list, scoring_path):
        """"""
        def _parse(directories_list):
            # scored_found exists only to build a return
            scored_found = {}
            for found in directories_list: # MUST BE A LIST
                _result = self._score_by_path(scoring_path, found)
                scored_found.update(_result)
                print("_result=", _result)
#                 input("Press enter...") #333
                # Try/except defacto checks for first addition of a list
                try:
                    # list item already exists
                    if self.searches[scoring_path]: self.searches[scoring_path].append(_result)
                except KeyError as e: self.searches[scoring_path] = [_result]
                except Exception as e:
                    err = "score_directory_list._parse: Unknown error attempting to add '[_result]' to self.searches. Skipping. \n(_result={R})".format(R = str(_result))
                    self._dump(err)
            
            return scored_found
                    
        if isinstance(directories_list, (list, tuple)):
            return _parse(directories_list)
        # Future
        # elif isinstance(directories_list, str):
            # pass
            
        elif isinstance(directories_list, _io.TextIOWrapper): # File handle
            _list = []
            for line in directories_list: _list.append(line)
            return _parse(_list)

        else:
            err = "score_directory_list: Unrecognized parameter type for 'directories_list' ({T}). Cannot parse. Skipping. ".format(T = str(type(directories_list)))
            self._dump(err)
        
    def search(self, path):
        _link_path = str(path)
#         scored_found = {}
        all_found    = [] # results of all finds
        # Search listings first
#         _link_dir  = ntpath.dirname(_link_path)
        _link_file = ntpath.basename(_link_path)
        for _archive_path in self._archive_listings:
#             print("(SL)", end="") #33333
#             _archive_dir  = ntpath.dirname(_archive_path)
            _archive_file = ntpath.basename(_archive_path)
            _archive_file = ntpath.basename(_archive_path).decode("utf-8")
            if _archive_file == _link_file:
#                 print(_archive_file, "v.", _link_file, "=", end="")
#                 print("!!!!!!!!!!!!!!MATCH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!") 
                all_found.append(_archive_path)
        # If there's a super good match (confirmed) skip searching direcctories
        # Then search directories
        if self.SEARCHHDD:
            for _start_dir in self.search_dirs:
#                 print("searching: {D} for link: {P}".format(D = _start_dir, P = str(_link_path))) #333
                # _result will be either 
                # a list of strings, each str a full path of a found file
                # Or False
                _result = self._search_dir(_start_dir, _link_path)
                if _result is False: continue 
    
                for found_path in _result:
                    # Sometimes extraneous directory delimiters sneak in. Remove
                    found_path = _delim + _delim.join(i for i in found_path.split(_delim) if len(i) > 0)
                    all_found.append(found_path)
    
        _result = self.score_directory_list(all_found, _link_path)
            # Use subprocess to do a find for filename in each search dir
            # Add founds (full path) to all_found
            # Score the found path against the original path to see if they are similar. 
            # Add all_found[original path] = {found_path:score}
            #===================================================================
            # for found in all_found:
            #     result = self._score_by_path(_path, found)
            #     scored_found.update(result)
            #     print("result=", result)
            #     input("Press enter...") #333
            #     # Try/except defacto checks for first addition of a list
            #     try:
            #         if self.searches[_path]: # list item already exists
            #              self.searches[_path].append(result)
            #     except KeyError as e:
            #         self.searches[_path] = [result]
            #===================================================================
        
        if _result: return _result
        else:      return False
            
    
    def _search_listings(self):
        pass
            
    def _search_dir(self, dir, path):
        """
        Search the directoris in seach_dirs for the filename. 
        The try to score it against the original file
        """
        print("(SD)", end="") #33333
        #  path must be a full path, strip to name
        _search_path = str(dir)
        _path = str(path)
        if not _path.startswith(_delim) or not _search_path.startswith(_delim): 
            err = "{C}.{M}: Either the directory or path are not a FULL PATH.\n(dir:'{D}')\n(path:'{P}'".format(C = self.__class__.__name__, M = stack()[0][3], P = _path, D = _search_path)
            self._dump(err)
            return [] 
        
#== Include directory names only===============================================
#         if _path.endswith(_delim):
#             err = "{C}.{M}: Path '{P}' does not seem to end in a filename.".format(C = self.__class__.__name__, M = stack()[0][3], P = _path)
# #             raise ValueError(err)
#             print(err)
#             return [] 
#===============================================================================
        _dir  = ntpath.dirname(_path)
        _file = ntpath.basename(_path)
#         scored_found = {}
        # First, search the text listings of the other archive disks
        
        # This searched the physical search dirs
        command = ["find", _search_path, "-name", _file]
        find_result = run(command, output = "list")
        #=======================================================================
        # result = []
        # for path in _result:
        #     # Sometimes extraneous directory delimiters sneak in. Remove
        #     path = _delim + _delim.join(i for i in path.split(_delim) if len(i) > 0)
        #     result.append(path)
        #=======================================================================
        #=======================================================================
        # # Add the new results to the all_found
        # all_found += result 
        #     # Use subprocess to do a find for filename in each search dir
        #     # Add founds (full path) to all_found
        #     # Score the found path against the original path to see if they are similar. 
        #     # Add all_found[original path] = {found_path:score}
        # for found in all_found:
        #     result = self._score_by_path(_path, found)
        #     scored_found.update(result)
        #     # Try/except defacto checks for first addition of a list
        #     try:
        #         if self.searches[_path]: # list item already exists
        #              self.searches[_path].append(result)
        #     except KeyError as e:
        #         self.searches[_path] = [result]
        # 
        #=======================================================================
        if find_result: return find_result
        else:           return False
         
                    
    def main(self):
        """"""
        # Results is a dict, key = the original link path
        self.results = {}
        #=======================================================================
        # while self.start_dir is None:
        #     # Assume command line call without parameter
        #     try:
        #         self.start_dir = input("Enter the FULL PATH of the directory to search:")
        #         if self.start_dir is None: raise AttributeError("'None' is not valid. ")
        #     except (ValueError,AttributeError) as e:
        #         err = "Invalid input ({E}). \nTry again.".format(E = str(e))
        #         self._dump(err)
        #=======================================================================
            
        for dir, link in findLinks(self.start_dir, use = "os"):
            self._dump()
            self._dump("=================")
            self._dump("path: {D}\nlinked-to: {L}".format(D = dir, delim = _delim, L = link)) 
            result = self.exists(link)
            if result["exists"]:
                self._yes_exists[dir] = result
                if not self.only_missing: self._dump("Exists at: '{P}'".format(P = result))
            else:
                self._dump ("Linked-to file does not exist.") 
                self._no_exists[dir] = link
                if self.dosearch:
                    print("Searching...".format(SD = self.start_dir, D = dir, delim = _delim, L = link))
                    if not self.search(link):
                        self._dump("!Nothing found!")
                    else:
                        self._no_exists.pop(dir)
        

        
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
