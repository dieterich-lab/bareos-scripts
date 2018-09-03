#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sys import argv
from os import walk
import pandas as pd
import re
import time

try: rootDir = argv[1]
except IndexError as e: rootDir = "." 

# FH = open("./search_nobackup_dirs.out", "w")
headers = ["Dir_name", "#Subdirs", "#Files", "#Suspect"]
global _header
_header = 0 
                  
def header():
    global _header
    if _header > 20: _header = 0 
    if _header == 0: 
        line = "{: <150} {: <8} {: <6} {: <8}".format(*headers)
        print() 
        print(line)
#         FH.write(line + "\n")
    _header += 1
        
def printit(_list):
    header()
    line = "{: <150} {: <8} {: <6} {: <8}".format(*_list)
    print(line)
#     FH.write(line + "\n")
    
for dirName, subdirList, fileList in walk(rootDir):

    if "scratch" in str(dirName): continue
     
    p = "trimmed.*reads|rrna"
    if len(fileList) > 0:
        if re.search(p, str(dirName).lower()):
            printit([dirName, str(len(subdirList)), str(len(fileList)), "ALL"])            
            continue
             
        suspect_count = 0
        for fname in fileList:
            p = "rrna|flexbar|_stargenome|_startmp"
            if re.search(p, str(fname).lower()): 
                suspect_count += 1
                
        if (suspect_count == len(fileList)): 
            printit([dirName, str(len(subdirList)), str(len(fileList)), str(suspect_count)])
            
print("DONE!") 
