#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from common.checks import pause

FH = open("/Users/mikes/Documents/Work/Heidelberg/projects/bareos/NOTES/FindArchivableRootDirs.20171108-511134.out", "r")
FHout = open("/Users/mikes/Documents/Work/Heidelberg/projects/bareos/NOTES/FindArchivableRootDirs.20171108-511134.csv", "w")
# Write headers
FHout.write("AGE in days,Directory")
for line in FH:
    line = line.split(":[")
#     print(line)
    if len(line) < 2: continue
    dir = line[0]
    if dir.count("/") > 3: continue
    age = line[1].split(",")[1]
    print(age, dir)
    FHout.write(age + "," + dir + "\n")
    
FH.close()
FHout.close()