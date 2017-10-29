#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from common.directory_tools import readLineGroup
from common.checks import Checks
checks = Checks()
_delim = checks.directory_deliminator()
import os
import subprocess
import ntpath

class MakeLink():
    def __init__(self, file):
        self.make_link(file)
    
    def _runcommand(self, command, test = False):
        """"""
        if test is False: 
#            print("RUNNING:", command)
           _result = subprocess.check_output(command).decode("utf-8")
        else:  
#            print ("Testing command (faking it):", command)
           _result = "Test OK"
        
        return _result
        
        
    def make_link(self, file):
        def _make_link():
            print("LINKING...")
#             print("LINKING: ", origdirname, "-->\n", copied_file, "\nto replace bad link:", bad_link)
            command = ["ln","-sf", copied_file, orig_file]
            _result = self._runcommand(command, test = False)
            try:
                _size = os.stat(orig_file).st_size
            except Exception as e:
                print("Failed to obtain linked file size. Something is amiss. !PLEASE CHECK MANUALLY! (ERR: {E})".format(E = str(e)))
                return False
            if _size !=copied_file_size:
                print("THE FILE SIZE DOES NOT MATCH THE LINK SIZE...!SOMETHING WENT WRONG!")
                return False
            
            _result = os.stat(orig_file)
            return _result
        # At this point everything shoud be in full paths except the original (bad) link
        for lines in readLineGroup(file, N = 10 ):
#             print("===============")
            orig_file = lines[0].strip() 
            if not orig_file.startswith(_delim): orig_file = _delim + orig_file
#             print(orig_file, "-->")
            command = ["ls", "-la", orig_file]
            _result = self._runcommand(command)
            print()
            print("Old-link:")
            print(_result)

            source_file = lines[7].split(" to ", 1)[0].split("'")[1].strip()
            if not source_file.startswith(_delim): source_file = _delim + source_file
            source_file_size = os.stat(source_file).st_size

            copied_file = lines[7].split(" to ", 1)[1].split("'")[1].strip()
            if not copied_file.startswith(_delim): copied_file = _delim + copied_file
            copied_file_size = os.stat(copied_file).st_size

            bad_link = lines[2]

            if source_file_size == copied_file_size: 
                _result = _make_link() # Automatically
            
            else:
                _answer = input("Continue?(y/N)")
                if "y" in str(_answer).lower(): _result = _make_link()
                else: print("Aborted.")

            print("Link command results:", _result)
            command = ["ls", "-la", orig_file]
            _result = self._runcommand(command)
            print("new-link:")
            print(_result)
    
    @classmethod
    def link_check(self, file):
        for lines in readLineGroup(file, N = 9 ):
            before_orig, before_link = lines[1].split(" -> ", 1)
            before_link = before_link.strip()
            before_orig = before_orig.split("/", 1)[1] 
            after_orig, after_link = lines[6].split(" -> ", 1)
            after_orig = after_orig.split("/", 1)[1]
            after_link = after_link.strip() 
            print(before_orig)
            #===================================================================
            # print("before_orig:", before_orig) 
            # print("after_orig :", after_orig) 
            # print("after_link:", after_link) 
            # print("before_link:", before_link) 
            # print("after_link :", after_link)
            #===================================================================
             
            before_orig_file = ntpath.basename(before_orig) 
            after_orig_file  = ntpath.basename(after_orig) 
            after_link_file  = ntpath.basename(after_link) 
            before_link_file = ntpath.basename(before_link) 
            after_link_file  = ntpath.basename(after_link)

            before_orig_dir = ntpath.dirname(before_orig) 
            after_orig_dir  = ntpath.dirname(after_orig) 
            after_link_dir  = ntpath.dirname(after_link) 
            before_link_dir = ntpath.dirname(before_link) 
            after_link_dir  = ntpath.dirname(after_link)
            
            if before_orig_dir == after_orig_dir and  before_orig_dir == after_link_dir[1:]:
                print("Directory paths...OK")
            else:
                print("Directory paths...!INCORRECT!")
            
#             print(before_orig_file)
#             print(after_link_file) 
            if before_orig_file in after_link_file :
                print("Filenames...OK")
            else:
                print("Filenames...!INCORRECT!")
#             count = 0
#             for i in lines:
#                 print (count, i)
#                 count += 1
                
        
if __name__ == '__main__':
#     o = MakeLink("./run.out")
    MakeLink.link_check("./linked.out") 
    
    