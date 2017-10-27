#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse       import ArgumentParser

import re

class ParseWeirdLinks():
    def __init__(self, parser = {}, *args, **kwargs):
        self._set_config(parser, args, kwargs)
        self.parse()
        
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
        parser.add_argument('--file', '-f', action="store", dest="FILE", type=str, 
                            default = None, help='File to search.')
        parser.add_argument('--matches', '-m', action="store", dest="MATCHES", nargs=2, 
                            default = [], type=int, help='Number of path matches.')
        parser.add_argument('--output', '-o', action="store", dest="OUTPUT", 
                            default=None, help='Location and name of the output file. ')
        parser.add_argument('--screendump', '-S', action="store", dest="SCREENDUMP", 
                            default=None, help='Dump output to screen as well as file. ')
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
        self.matches      = kwargs.pop("MATCHES", [2,99])
        print("self.matches = ", self.matches )
        if self.matches == []: self.matches = (1, 99)
        self.SCREENDUMP     = kwargs.pop("SCREENDUMP", True)
        if self.SCREENDUMP: self.SCREENDUMP = True # Force to boolean if needed
        self.file      = kwargs.pop("FILE", ".")
        self.RESULTS = {}


    def parse(self):
        pmatches = ''.join(["\[-1(, -1)*(, 0){", str(self.matches[0]), ",", str(self.matches[1]),"}\]"])
#         pmatches = ".*" 
        with open(self.file, "r") as INFH:
            print_flag = False
            for line in INFH:
                line = line.strip()
                if re.search("->", line):                    
                    if re.search("\(\d*\).*", line):  # Meaning found files
                        print("==========")
                        print(line)
                        print_flag = True
                
                    else: 
                        print_flag = False
                
                elif re.search(pmatches, line) and print_flag is True: 
                    print(line)
                    
    def different_search(self):
        orig = ""
        link = ""
        found = ""
        try: FH = open(infile,'rb')
        except Exception as e:
            err = "Unknown error attempting to open '{F}' as a binary file. (ERR: {E})".format(F = str(infile), E = str(e))
            raise ValueError(err)
        
        try: self.RESULTS = pickle.load(FH)
        except Exception as e:
            err = "Unknown error attempting to un-pickle file '{F}'. Is it a pickle file? (ERR: {E})".format(F = str(infile), E = str(e))
            raise ValueError(err)
        
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
            
        #=======================================================================
        #     _found_sorted = sorted(_found, key=self._rank_by_score, reverse = True)
        #     # Replace original list with sorted list for ater pickle
        #     self.RESULTS[key]["found"] = _found_sorted 
        #     if outfile:         self.writeall(outfile = outfile, matches = matches, num_score = num_score)
        #     if self.SCREENDUMP: self.printall(matches = matches, num_score = num_score)
        # 
        #=======================================================================
            
            for _file, _score in _found.items():
                _orig = key.replace("/", "")
                _file = _file.replace("/", "")
                
                
                     
        
    
                
if __name__ == '__main__':
    parser = ArgumentParser()
    object = ParseWeirdLinks(parser)
            