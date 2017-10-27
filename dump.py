#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pickle
import ntpath
import os
from common.runsubprocess import RunSubprocess as run
file = "findArchiveDiskLinks-RESULTS-1509011909.433804"
ALLOUTFH = open(file + "all.out", "w")
NOTHINGOUTFH = open(file + "nothingfound.out", "w")
EXISTSOUTFH = open(file + "exists.out", "w")
FOUNDOUTFH = open(file + "found.out", "w")

FH = open(file + ".pkl",'rb')
RESULTS = pickle.load(FH)

for k,v in RESULTS.items():
    try: _found = str(v["found"])
    except KeyError as e: 
        msg = '(KeyError opening v["found"])'
        print(msg)
        ALLOUTFH.write(msg + "\n")
        continue
    
    # Fubar fix ========================================
    if len(v["found"]) == 1:
        link = v["link"]
        dir = ntpath.dirname(k)
        if not dir.startswith("/"): dir = "/" + dir
        _path = dir + "/" + v["link"]
        _exists= os.path.exists(_path)
        if _exists: v["exists"] = True
    # Fubar fix ========================================

    msg = k + "->" + v["link"]
    if v["exists"] == True:
        msg = "(OK)" + msg
        print(msg)
        ALLOUTFH.write(msg + "\n")
        EXISTSOUTFH.write(msg + "\n")
        continue

    else:
                    
        if ("NOTHING_FOUND" in _found) or ("exists" in _found) or (v["found"] == []):
            msg = "(NOTHING_FOUND)" + msg
            print(msg)
            ALLOUTFH.write(msg + "\n")
            NOTHINGOUTFH.write(msg + "\n")
            continue
        
        else:
            msg = ''.join(["(", str(len(v["found"])), ")", msg])
            print(msg)
            ALLOUTFH.write(msg + "\n")
            FOUNDOUTFH.write(msg + "\n")

            for i in v["found"]:
                msg = ''.join([k, "(LINK)", v["link"],"(FOUND)",i[0], "(SCORE)", str(i[1])])
                print(msg)
                ALLOUTFH.write(msg + "\n")
                FOUNDOUTFH.write(msg + "\n")

ALLOUTFH.close()
NOTHINGOUTFH.close()
EXISTSOUTFH.close()
FOUNDOUTFH.close()

            
        
