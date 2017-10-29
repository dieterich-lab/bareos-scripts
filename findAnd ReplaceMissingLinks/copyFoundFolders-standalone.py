#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from common.checks import Checks
checks = Checks()
from datetime import datetime as time
from distutils.dir_util import copy_tree
from common.runsubprocess import RunSubprocess as run

import atexit
import ntpath
import shutil
import os
import re
import subprocess

link = path = ""
result_lines = [] 
OUT = open("./DieterichLab_archive_disk_3-linksearch.COPIED.out", "w+")

class CopyGroupCDfolder():
    def __init_(self):
        pass
    
    def _runcommand(self, command, test = True):
        """"""
        if test: 
           print("RUNNING:", command)
           _result = subprocess.check_output(command).decode("utf-8")
        else:  
           print ("FAKING IT:", command)
           _result = ""
        
        if _result == "":
            print("RESULT='{R}' (null means 'OK'".format(R = _result))
            return True
        else:
            err = "Something went wrong with command. Halting. (result = '{R}').".format(R = str(_result))
            print(err)
        #              raise RuntimeError(err)
            return False
        
    def renameCopied(self, file):
        from itertools import zip_longest
        
        def grouper(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=fillvalue)
        
                
#         inFH = open(file, "r")
        # read to list first, to remove extraneous from the "human readable" output
        _lines = []
        with open('C:/path/numbers.txt') as f:
            for line in f.read().splitlines():
                if "Adding" in line:    continue # skip
                if len(line) < 1:       continue # skip
                _lines.append(line)
        
        N = 7
        with open(file) as f:
             for lines in grouper(f, N, ''):
                 assert len(lines) == N
                 try:
                     if not "Copied:" in lines[0]:
                         err = "Something's out of whack. First line entry should be 'Copied:' header. ({L})".format(L = str(lines))
                         raise ValueError(err)
                     copiedsrc = lines[1].rstrip("\n").rstrip("\r")
                     copieddst = lines[3].rstrip("\n").rstrip("\r")
                     replacelink = lines[5].rstrip("\n").rstrip("\r")
                     sizefrom, sizeto = lines[6].split(" to ")
                     linkrootdir = ntpath.dirname(replacelink)
                     #==========================================================
                     # print("linkrootdir:", linkrootdir)
                     # print("copiedsrc:", copiedsrc)
                     # print("copieddst:", copieddst)
                     # print("replacelink=", replacelink)
                     # print("sizefrom:", sizefrom) 
                     # print("sizeto:", sizeto)
                     # print("copiedsrc:", copiedsrc)
                     #==========================================================
                     copied_dst_src_path = linkrootdir + "/" + "COPY-OF" + '-'.join(copiedsrc.split("/"))
#                      print("copied_dst_src_path:", copied_dst_src_path)
                     if  not checks.pathExists(copieddst):
                         err = "Unable to verify file '{F}'. Aborting this segment."
                         print(err.format(F = copieddst))
                         continue
                     try: _result = subprocess.check_output(["ls", "-lad", replacelink])
                     except subprocess.CalledProcessError as e:
                        err = "Unable to verify file '{F}' due to a Non-zero status from 'ls'. Aborting this segment."
                        print(err.format(F = replacelink))
                        print(str(e))
                        continue
                     print("_result=", _result)
                     if not len(copied_dst_src_path) > len(copiedsrc):
                         err = """Somethings wrong with the #copied_dst_src_path# filenmae basd on the copied file path...
                                  CDSP: {C}
                                  Copied path: {P}
                                  Aborting this segment.""".format(CDSP = copied_dst_src_path, P = copiedsrc)
                         print(err)
                         continue
                     
                     print()
                     print("Correcting link: '{L}' to data copied from '{F}' as '{C}'".format(L = replacelink, F = copiedsrc, C = copied_dst_src_path))
                     # First rename the wrongly named file. Halt if fails
                     if not self._runcommand(["mv", copieddst, copied_dst_src_path]): continue
                     # The remove the link. Its OK if this fails. 
                     if not self._runcommand(["rm", "-rf", replacelink]): pass
                     if not self._runcommand(["ln", "-sf", copied_dst_src_path, replacelink]):
                         err = "LINK NOT REPLACED. Manually replace with 'ln -sf {F}  {L}".format(F = copied_dst_src_path, L = replacelink)
                         print(err)
                 
                 except ValueError as e:
                     print("Done.")
                    
                 
                 
#         while not EOF:
#             line = inFH.readline()
#             print (line)
#             if "Copied:" in line: pass
#                 
            
        
        
    def _parser(self, link, path, result_lines):
        
        path = path.replace("\n", "").replace("\r", "")
        if link == "" or path is "" or len(result_lines) < 1: return False
        p = "\[(-1, -1)(, 0)*\]" # KNown good matches
        for i in result_lines:
            foundpath, matches = i.split(":")
            # For now, just the known good
            if re.match(p, matches):
                print("==========================") #333
                print("path=", path) #333
                print("link=", link) #333
                print("search results:") #333
                for i in result_lines: #3333
                    print (i) #3333
                print()
                print("path:", foundpath)
                print("Qualifies with a match of:", matches)
                now = time.now()
                hrnow = (now.strftime('%Y-%m-%d-%H-%M-%S'))
                movepath = path + "_OLDLINK_" + hrnow
                archivedpath = foundpath + "_ARCHIVED_" + hrnow
                copytopath = path + ".COPIED"
                
                print("copy: ")
                print(foundpath)
                print("to\n")
                print(copytopath)
                print("...and move...")
                print(link)
                print("to\n")
                print(movepath)
                print("...and move...")
                print(foundpath)
                print(archivedpath)
               
               
               #====================================================================
               # print("Moving: \n", path, "to\n", movepath)
               # input("Press enter to move...")
               # command = ["mv", path, movepath]
               # try:
               #     result = run(command, verbose = True)
               #     print(result)
               # except Exception as e:
               #      print("OOps: Error occurred attempting to move the original (bad) link \n({P}) \nto a new name\n({M}).\nCANCELING AND CONTINUING! \n(Err: {E}).".format(P = path, M = movepath, E = str(e)))
               #      return # from _parser, but continue
               #====================================================================
    #            shutil.move(path, movepath)
    #===============================================================================
    #            print("Copying:\n", foundpath, "to\n", copytopath)
    #            if os.path.isfile(foundpath):
    # #                input("Press enter to copy...")
    #                shutil.copy2(foundpath, copytopath)
    #            elif os.path.isdir(foundpath):
    # #                input("Press enter to copy...")
    #                copy_tree(foundpath, copytopath, preserve_mode=1, preserve_times=1, preserve_symlinks=1, verbose=1, dry_run=0)
    #            else:
    #                 err = "Could not figure out if file or directory. ({P})".format(P = foundpath)
    #                 raise RuntimeError(err)
    #            # Assuming success here
    #            orig_size = subprocess.check_output(['du','-s', foundpath]).split()[0].decode('utf-8')
    #            copied_size = subprocess.check_output(['du','-s', copytopath]).split()[0].decode('utf-8')
    #            print(orig_size, "to", copied_size)
    #            OUT.write("Copied:\n")
    #            OUT.write(foundpath + "\n")
    #            OUT.write("to\n")
    #            OUT.write(copytopath + "\n")
    #            OUT.write("to replace link:\n")
    #            OUT.write(path + "\n")
    #            OUT.write(orig_size + " to " + copied_size + "\n")
    # #            if (orig_size * 1.02 < copied_size) and (orig_size * 0.98 > copied_size):
    # #                print("I think size is OK." )
    # #            input("OK to move the foundpath(" + foundpath + ") to ARCHIVED STATUS? (Enter = yes)?")     
    # #            print("Moving:\n", foundpath, "to\n", archivedpath)
    # #            shutil.move(foundpath, archivedpath)
    #            return True # ONly one copy needed per parser call. 
    #         else:
    #             continue
    #         
    #===============================================================================
    #     print("link:",  link)
    #     print("path:", path)
    #     print(result_lines)
    def parseFoundFiles(self):
        with open("./DieterichLab_archive_disk_3-linksearch.out", "r") as FH:
            for line in FH:
        #         print(line)
                if re.match("^(\=)*$", line):
                    self._parser(link, path, result_lines)
                    # Reset
                    link = path = ""
                    result_lines = [] 
                    continue 
                
                elif re.search('(Does not exist).*', line, re.IGNORECASE): continue 
                elif re.search("(Searching)(\.)*.*", line, re.IGNORECASE): continue
                elif re.search('!Nothing found!', line, re.IGNORECASE): continue
                
                elif "link:" in line:
                    link = line.replace("link:", "")
        #             print("link:", link)
                elif "path:" in line: 
                    path = line.replace("path:", "")
        #             print("path:", path)
                else:
                    result_lines.append(line)
                    
if __name__ == "__main__":
    o = CopyGroupCDfolder()
    o.renameCopied("DL3.COPIED.out")