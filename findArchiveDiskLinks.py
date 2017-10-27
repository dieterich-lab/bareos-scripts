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
from common.printdots import Printdots
dots = Printdots()
from collections import OrderedDict
from operator import itemgetter   
from inspect import stack

import atexit
import ntpath
import os
import pickle
import re
import time


class FindArchiveDiskLinks(object):
    def __init__(self, parser = {}, *args, **kwargs):
        self._set_config(parser, args, kwargs)
        atexit.register(self._cleanup)
                
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
                            default = None, help='Starting directory for search.')
        parser.add_argument('--search_dirs', '-D', action="store", dest="SEARCH_DIRS", nargs='*', 
                            default = None, help='Directories in which to search for missing files. ')
        parser.add_argument('--output', '-o', action="store", dest="OUTPUT", 
                            default=None, help='Location and name of the output file. ')
        parser.add_argument('--screendump', '-S', action="store", dest="SCREENDUMP", 
                            default=None, help='Dump output to screen as well as file. ')
        parser.add_argument('--search', '-s', action="store", dest="SEARCH", 
                            default=None, help='Search for the missing link. If True, script searches for the missing link. If False, it simply reports it as missing. ')
        parser.add_argument('--search-hard-drive', action="store", dest="SEARCHHDD", 
                            default=None, help='Search for the missing link. If True, script searches for the missing link. If False, it simply reports it as missing. ')
        parser.add_argument('--only-missing', action="store", dest="ONLYMISSING", 
                            default=None, help='Display only links with a missing linked file. ')
        parser.add_argument('--include-good', action="store", dest="INCLUDEGOOD", 
                            default=None, help='Display only links with a missing linked file. ')
        parser.add_argument('--pickle', action="store", dest="PICKLE", 
                            default=None, help="Python 'Pickle' the search output, instead of text.")
        parser.add_argument('--verbose', "-v", action="store", dest="VERBOSE", 
                            default=0, help="Lowest verbose level (1)")
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
        # VERBOSE MUST COME FIRST
        self.verbose      = kwargs.pop("VERBOSE", 0) 
        self.SCREENDUMP     = kwargs.pop("SCREENDUMP", True)
        if self.SCREENDUMP: self.SCREENDUMP = True # Force to boolean if needed
        self.STARTTIME = time.time()
        self.start_dir      = kwargs.pop("START_DIR", ".")
        self.search_dirs    = kwargs.pop("SEARCH_DIRS", ["/beegfs", "/mnt/group_cd"])
        self.output         = kwargs.pop("OUTPUT", 'findArchiveDiskLinks-RESULTS-' + self.start_time)
        self.dosearch       = kwargs.pop("SEARCH", True)
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
        # Load all the directory paths from the archive disk listings
        self.RESULTS = {}
        
    def _cleanup(self):
        exit_msg = "Cleaning up {C}.".format(C = self.__class__.__name__)
        self.printout(exit_msg)
        # Pickle 
        if len(self.RESULTS) > 0 and self.PICKLE == True:
            _dir = os.getcwd()
            _path = self.output + ".pickle"
            msg = ("Dumping searches as pickle to '" + _path + "' ...")
            try:
                with open(_path, 'wb') as f:
                    pickle.dump(self.RESULTS, f)
                    msg += "OK"
            except Exception as e:
                msg += "FAILED (ERR:{E})".format(E = str(e))
            self.printout(msg)

            # also dump as text
            _path = self.output + ".text"
            self.printout("Dumping searches as text to '" + _path + "' ...")
            self.writeall(outfile = _path)
            #===================================================================
            # f = open(_path, 'w')
            # for key, value in self.RESULTS.items():
            #     try: link = self.RESULTS[key]["link"]
            #     except KeyError: link = "!Link_Key_Error!"
            #     
            #     try: exists = self.RESULTS[key]["exists"]
            #     except KeyError: exists = "!Exists_Key_Error!"
            #     
            #     try: found = self.RESULTS[key]["found"]
            #     except KeyError: found = []
            #     
            #     msg = self._dump_message(key, link, exists, found, dump_found = True)                
            #     #===============================================================
            #     # msg = str(key) + "->"
            #     # for k in "link", "exists", "found":
            #     #     try: msg += str(self.RESULTS[key][k])
            #     #     if not isinstance(self.RESULTS[key][k], (list, tuple)):
            #     #         try: 
            #     #             f.write("{k}:{V}\n".format(k = k, V = self.RESULTS[key][k]))
            #     #         except Exception as e: 
            #     #             f.write("{k}:{V}\n".format(k = k, V = "UNKNOWN_ERROR", E = str(e)))
            #     #     else:
            #     #         f.write("{k}:\n".format(k = k))
            #     #         for l in self.RESULTS[key][k]:
            #     #             try: 
            #     #                 f.write(str(l) + "\n")
            #     #             except Exception as e: 
            #     #                 f.write("UNKNOWN ERROR WRITING LIST ITEM ({E})\n".format(E = str(e)))
            #     #===============================================================
            #     try: f.write(msg + "\n")
            #     except Exception as e: 
            #         f.write("FILE:{K}({E})\n".format(K = "ERROR", E = str(e)))
            #===================================================================
                                
            msg += "DONE."
            f.close()                
            self.printout(msg)
                    
    def printout(self, *args, **kwargs):
        """"""
        if len(args) < 1: return # Nothing passed
        _verbose = kwargs.pop("VERBOSE", 0)

        _args = ' '.join(str(s) for s in args)
        if self.SCREENDUMP and (_verbose <= self.verbose):
            try: 
                print(_args)
        
            except UnicodeEncodeError as e:
                if re.search(".*ascii.*can't\s*encode.*ordinal\s*not\s*in\s*range", _args): 
                    print(_args.encode('utf8'))

    def _dump_message(self, dir, link, exists, all_found, dump_found = False,  matches = False, num_score = False):
        _dump_msg = str(dir).strip() 
        _dump_msg += "->"
        _dump_msg += str(link).strip()
        if exists is True: # "True" not just set other than False
            _dump_msg = "(OK)" + _dump_msg
        elif "skip" in str(exists).lower() : # "True" not just set other than False
            _dump_msg = "(SKIPPED)" + _dump_msg
        else:
            if len(all_found) < 1:
                _dump_msg = "(!NOTHING_FOUND!)" + _dump_msg
            else:
                _dump_msg = "(" + str(len(all_found)) + ")" + _dump_msg
                if dump_found is True:
                    for _found in all_found:
                        if matches:
                            p = "\[(-1)(, -1)*(, 0){" + str(matches) + ",}\]"
                            if re.search(p, str(_found)): 
                                _dump_msg +="\n" + str(_found)
                        if num_score:
                            if self._rank_by_score(_found) >= num_score:                            
                                _dump_msg +="\n" + str(_found)
                            
        return _dump_msg
    
    def _rank_by_score(self, scorelist):
        _path = scorelist[0]
        _scorelist = scorelist[1]
        if re.search("/*mnt/group_cd/.*", _path):
            _scorelist = scorelist[2:]
        p = "\[(0|-1){1,1}(, -1)*(, 0)*\]"
        #=======================================================================
        # if re.search(p, str(_scorelist)):
        #     print("Setting to 1") 
        #     _total = len(_scorelist)
        # else:
        #     print("Setting to 0") 
        #     _total = 0
        #=======================================================================
        _total = 0
        for i in range(len(_scorelist) - 1, 0, -1):
            _value = 1 if _scorelist[i] is 0 else -1
            _total += ((len(_scorelist) - i) * (_value))
        return _total
        
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
        result = [found,score_l]
        self.printout(result, VERBOSE = 3)
        return result

    @property
    def outfile(self):
        try:
            return self.OUTFILE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)

    @outfile.setter
    def outfile(self, value):
        _value = str(value)
        if not _value.startswith(_delim):
            _value = os.getcwd() + _delim + _value
        self.OUTFILE = _value
        self.printout("'{A}' set to '{V}'".format(A = str(stack()[0][3]), V = self.OUTFILE), VERBOSE = 2)

    @outfile.deleter 
    def outfile(self):
        del self.OUTFILE

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
    
        self.printout("'{A}' set to '{V}'".format(A = str(stack()[0][3]), V = self.START_DIR), VERBOSE = 2)

    @start_dir.deleter
    def start_dir(self):
        del self.START_DIR

    @property
    def start_time(self):
        try:
            return str(self.STARTTIME)
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)        

    @start_time.setter 
    def start_time(self, value):
        err = "Attribute {A} cannot be manually set. ".format(A = str(stack()[0][3]))
        raise ValueError(err)    
         
    @start_time.deleter 
    def start_time(self):
        pass
                    
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
        self.printout("'{A}' set to '{V}'".format(A = str(stack()[0][3]), V = self.SEARCH_DIRS), VERBOSE = 2)
                     
    @search_dirs.deleter
    def search_dirs(self):
        del self.SEARCH_DIRS
    
    @property
    def verbose(self):
        try:
            return self.VERBOSE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
            raise ValueError(err)            

    @verbose.setter
    def verbose(self, value):
        _value = str(value).lower()
        try:                _value = int(_value)
        except ValueError:  _value = _value.count('v')
        self.VERBOSE = _value
    
    @verbose.deleter
    def verbose(self):
        del self.VERBOSE
        
    def exists(self, path, link):
        """
        Must return a dict
        """
        _path = str(path).strip()
        _link = str(link).strip()
        # path should always be full path
        if not _path.startswith(_delim): _path = _delim + _path
        if not _link.startswith(_delim):
            # It's relative to _path
            _dir = ntpath.dirname(_path) 
            _link = _dir + _delim + _link
        # If it exists, gather info        
        if os.path.exists(_link):
            _stats = str(os.stat(_link)) 
            self.printout("'{L}' exists ({S})".format(L = _link, S = _stats), VERBOSE = 3)
            return _stats
        else: 
            self.printout("'{L}' DOES NOT EXIST", VERBOSE = 3)
            return False
                
    def find(self):
        """"""
        # Load up the disk listings from the archive disk text files
        self.PICKLE = True # Turn on _cleanup pickle
        self._listings = []
        if self.dosearch:
            for file in os.listdir("/beegfs/prj/archive_disks"):
                _path = "/beegfs/prj/archive_disks/" + file
                self.printout("Loading listings for:", _path, VERBOSE = 1)
                try:
                    with open(_path, "rb") as fh:
                        for line in fh: 
                            if isinstance(line, bytes): line = line.decode("utf-8")
                            line = str(line).strip()
                            if line.startswith("."): line = line.replace(".", _path + _delim, 1)
                            # Removes blank directory delimiters
                            _list = line.split(_delim)
                            line = _delim + _delim.join(i for i in _list if len(i) > 0) 
                            self._listings.append(line)
                except (FileNotFoundError, PermissionError) as e:
                    err = "Unable to open '{P}'. Skipping. ".format(P = _path)
                    self.printout(err)
    
            # Load listings of the HDD paths        
            if self.SEARCHHDD:
                for _dir in self.search_dirs:
                    self.printout("Loading listings for:", _dir , VERBOSE = 1)
                    command = ["find", _dir]
                    find_result = run(command, output = "list")
                    # The results come in with inconsistent encoding. Clean
                    for line in find_result: 
                        if isinstance(line, bytes): line = line.decode("utf-8")
                        line = str(line).strip()
                        # Removes blank directory delimiters
                        _list = line.split(_delim)
                        line = _delim + _delim.join(i for i in _list if len(i) > 0)
                        self._listings.append(line)
        
        for dir, link in findLinks(self.start_dir, use = "os"):
            # Dir is the RESULTS key
            dir = str(dir).strip()
            self.RESULTS[dir] = {}
            # Link next
            link = str(link).strip()
            self.RESULTS[dir]["link"] = link
            self.printout("CHECKING: {D}-> {L}".format(D = dir, L = link), VERBOSE = 2) 
            ### Parse 
            # Skip common uneeded files
            # "skips" are regex SEARCHES (not matches)
            skips = ["__OLD__SOFTWARE__","python\d\.\d", "python-\d\.\d\.\d", "R-\d\.\d\.\d", "s{0,1}ratoolkit\.\d\.\d\.\d\-\d"]
            for skip in skips:
                if (re.search(skip, dir)) or (re.search(skip, link)):
                    self.RESULTS[dir]["exists"] = "Skipped"
                    self.printout("SKIPPED: Due to '{R}'".format(R = skip), VERBOSE = 3)
                    continue 
            # Check if link exists. If so, nect
            result = self.exists(dir, link)
            if result is not False: # "True" not just set other than False
                self.RESULTS[dir]["exists"] = True
                self.printout("'exists' = '{V}'".format(V = str(self.RESULTS[dir]["exists"])), VERBOSE = 3)
                self.RESULTS[dir]["stat"]   = result
                self.printout("'stat' = '{V}'".format(V = str(self.RESULTS[dir]["stat"])), VERBOSE = 3)
                self.RESULTS[dir]["found"]  = [["exists", []]] # Backward compatibility
                self.printout("'found' = '{V}'".format(V = str(self.RESULTS[dir]["found"])), VERBOSE = 3)

            else:
                self.RESULTS[dir]["exists"] = False                
                if self.dosearch:
                    all_found = self.search(dir, link)
                    if len(all_found) < 1:
                        self.RESULTS[dir]["found"] = [["!NOTHING_FOUND!", []]] # For compatibility
                        self.printout("'found' = '{V}'".format(V = str(self.RESULTS[dir]["found"])), VERBOSE = 3)
                    else:
                        self.RESULTS[dir]["found"] = all_found
                        self.printout("'found' = '{V}'".format(V = len(str(self.RESULTS[dir]["found"]))), VERBOSE = 3)
                         

    def printall(self, matches = 2, num_score = False):
        for key, value in self.RESULTS.items():
            try: link = self.RESULTS[key]["link"]
            except KeyError: link = "!Link_Key_Error!"
            
            try: exists = self.RESULTS[key]["exists"]
            except KeyError: exists = "!Exists_Key_Error!"
            
            try: found = self.RESULTS[key]["found"]
            except KeyError: found = []
            
            msg = self._dump_message(key, link, exists, found, dump_found = True, matches = matches, num_score = num_score)                
            try: print(msg)
            except Exception as e:
                err = "!ERROR! Could not write line to std out. ({E})".format(E = str(e))
                print(err)
        
    def sort(self, infile = False, outfile = False, by = "score", matches = 2, num_score = False):
        """
        Sort the results of a find, from the file output or dict.
        Returns dicts. Also writes TEXT to an outfile if given. 
        If infile is False, the internal dict is used...however this only works 
        if the internal dict is population (I.e. find than run and called before
        program close...or the pickly object is passed in. 
        """        
        def _load_err(k, d, s, e):
            err = "Something is amiss for {K}:{D} since there should always be a '{S}' key. ({E})".format(K = str(k), D = str(d), S = str(s), E = str(e))
            print(err)
            
        if infile:
            if isinstance(infile, dict):
                 self.RESULTS = infile

            elif os.path.isfile(infile):
                try: FH = open(infile,'rb')
                except Exception as e:
                    err = "Unknown error attempting to open '{F}' as a binary file. (ERR: {E})".format(F = str(infile), E = str(e))
                    raise ValueError(err)
                
                try: self.RESULTS = pickle.load(FH)
                except Exception as e:
                    err = "Unknown error attempting to un-pickle file '{F}'. Is it a pickle file? (ERR: {E})".format(F = str(infile), E = str(e))
                    raise ValueError(err)

                # Reset outfile to match the input file                
                _matches = re.search("(.*)(\.pickle.*|\.text.*|\.pkl.*|\.txt.*)*", str(infile), re.IGNORECASE)
                if _matches is not None:
                    self.outfile = _matches.groups()[0]
                else:
                    err = "Failure regex matching '{I}'".format(I = str(infile))
                    raise RuntimeError(err)
                                    
        else: # infile false
            if self.RESULTS == {}:
                err = "Unable to grab RESULTS dictionary from self or parameter 'infile'. Please re-run the sort method after running find(), or passing in a valid dictionary or the file path for a valid dictionary pickle. "
                raise RuntimeError(err)
            # Otherwise, self.RESULTS should already be populated.
        
        try: matches = int(matches)
        except:
            err = "Parameter 'matches' must be an integer. (value = {V})".format(V = str(matches))
            raise ValueError(err)

        if outfile is True: 
            outfile = ''.join([self.output, ".", str(i), "-sorted.text"])
        elif checks.isPathFormat(outfile):
            outfile = outfile
        else:
            outfile = False
            
            #===================================================================
            # try: OUTFH = open(outfile, "w")
            # except Exception as e:
            #     err = "UNknown error attempting to open outfile '{O}'".format(O = str(outfile))
            #     raise IOError(err)                
            #===================================================================

        # MAIN ==============================
        for key, data in self.RESULTS.items():
           # --------------------------- 
            try: _path = key.strip()
            except (KeyError) as e:
                _load_err(key, data, "(Original_File) Key", e)
                continue
           # ---------------------------             
            try: _linked_to_path = self.RESULTS[key]["link"].strip()
            except (KeyError) as e:
                _load_err(key, data, "link", e)
                continue
           # --------------------------- 
            try: 
                _exists = self.RESULTS[key]["exists"]
            except (KeyError) as e:
                _load_err(key, data, "exists", e)
                continue
           # --------------------------- 
            try: 
                _found = self.RESULTS[key]["found"]
            except (KeyError) as e:
                _load_err(key, data, "found", e)
                continue
            #################################################################
            # Fix inconsistency with how "found" was tracked. Can be removed
            if _found == [] or ("NOTHING_FOUND" in _found):
                if _exists == True:
                    self.RESULTS[key]["found"] = _found = [["exists", []]] # For compatibility
                else:
                    self.RESULTS[key]["found"] = _found = [["!NOTHING_FOUND!", []]] # For compatibility                    
                continue
            #################################################################
            if ("NOTHING_FOUND" in _found) or ("exists" in _found):
                continue
            _found_sorted = sorted(_found, key=self._rank_by_score, reverse = True)
            # Replace original list with sorted list for ater pickle
            self.RESULTS[key]["found"] = _found_sorted 
            if outfile:         self.writeall(outfile = outfile, matches = matches, num_score = num_score)
            if self.SCREENDUMP: self.printall(matches = matches, num_score = num_score)

    def search(self, orig_file_path, link_path, both = True):
        """"""
        def _search(path):
            _dir  = ntpath.dirname(path)
            _file = ntpath.basename(path)
            _num = re.search("(archive_disk_)(\d)(_contents)", orig_file_path)
            try:
                _num = _num.groups()[1]
                int(_num)
            except ValueError as e:
                err = "search: regex for original file's archive disk number ended up invalid (number:{N}). Halting. !Please check!".format(F = str(_num))
            for _listing_path in self._listings:
                # Dont search the content listings for the same disk were investigating
                if re.search("archive_disk_{N}_contents".format(N = str(_num)), _listing_path): 
                    continue
                
                _listing_dir  = ntpath.dirname(_listing_path)
                _listing_file = ntpath.basename(_listing_path)
                if _listing_path.endswith(_file):
                    _append = self._score_by_path(link_path, _listing_path)
                    print("Appending:", _append)
                    all_found.append(_append)
            
        _link_path = str(link_path).strip()
        all_found    = [] # results of all finds
        self.printout("SEARCHING FOR LINK:", link_path, VERBOSE = 2)
        _search(link_path)
        self.printout("SEARCHING FOR ORIGINAL:", orig_file_path, VERBOSE = 2)
        _search(orig_file_path)
        
        return all_found
    
    def writeall(self, outfile, matches = 2, num_score = False):
        try: FHOUT = open(outfile, "w")
        except Exception as e:
            err = "Unable to open file '{F}'. (!ERROR!: {E})".format(F = str(outfile), E = str(e))
            raise IOError(err)
        
        for key, value in self.RESULTS.items():
            try: link = self.RESULTS[key]["link"]
            except KeyError: link = "!'link'_Key_Error!"
            
            try: exists = self.RESULTS[key]["exists"]
            except KeyError: exists = "!'exists'_Key_Error!"
            
            try: found = self.RESULTS[key]["found"]
            except KeyError: found = ["!'found'_Key_Error!"]
            
            msg = self._dump_message(key, link, exists, found, dump_found = True, matches = matches, num_score = num_score)                
            try: FHOUT.write(msg + "\n")
            except Exception as e:
                err = "!ERROR! Could not write line to {F} out. ({E})".format(F = str(FHOUT), E = str(e))
                print(err)
                
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = FindArchiveDiskLinks(parser)
