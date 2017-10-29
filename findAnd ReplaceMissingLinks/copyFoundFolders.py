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
# ConfigHandler disabled until py3 update
# from common.confighandler  import ConfigHandler 
# loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
# from common.loghandler     import log 
from common.checks         import Checks
checks = Checks()
_delim = checks.directory_deliminator()
from common.directory_tools import readLineGroup
from inspect import stack
from ast import literal_eval
from shutil import copy2
from time import time

import atexit
import ntpath
import os
import re

class CopyFoundFolders(object):
    """
    pickle: 
        archive_path = The filename in the archive disk.

        link = The non-existant path that the archive_path was pointing to 

        found = a list of dicts, containing the path of a found file, and 
                its score as it relates to the path of the link.

        _pickle[archive_path] = /mnt/archiveDisk/dir1/dir2/filename    
        _pickle[archive_path]["link"] = /linkdir1/linkdir2/filename 
        _pickle[archive_path]["found"]= [
                                {/wrongdir1/wrongdir2/filename:[-1, -1, 0]}, 
                                {/rightdir1/rightdir2/filename:[0, 0, 0]}, 
                                etc.
                                ]
    """    
    def __init__(self, parser = {}, *args, **kwargs):
        self._set_config(parser, args, kwargs)
#         atexit.register(self._cleanup)
        self.main()
                
    def _pickleit(self, obj):
        _path = os.getcwd() + _delim + "copyFoundFolders-run-" + str(time()) + ".pickle"
        msg = ("Dumping searches as pickle...")
        try:
            import pickle as PICKLE
            with open(_path, 'wb') as f:
                PICKLE.dump(obj, f)
                msg += "OK"
        except Exception as e:
            msg += "FAILED (ERR:{E})".format(E = str(e))
        print(msg)
                    
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
        parser.add_argument('--file', '-f', action="store", dest="FILE", type=str, default = None, required = True, help='Starting directory for search.')
        return parser
    
    def _set_config(self, parser, args, kwargs):
        """"""
        # Set class-wide
        self.app_name = self.__class__.__name__
#         self.CONF   = ConfigHandler()# ConfigHandler disabled until py3 update
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
        
        # #=== loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
        # # Here we parse out any args and kwargs that are not needed within the self or self.CONF objects
        # # if "flag" in args: self.flag = something
        # # Logging
        # self.logfile    = kwargs.pop('log_leveL', 'system') # Default warning
        # self.log_level  = kwargs.pop('log_leveL', 10) # Default warning
        # self.screendump = kwargs.pop('screendump', True) # Default off
        # self.formatter  = kwargs.pop('formatter', '%(asctime)s-%(name)s-%(levelname)s-%(message)s')
        # self.create_paths = kwargs.pop('create_paths', True) # Automatically create missing paths
        #=======================================================================
        # parser stuff
        self.file = kwargs.pop("FILE", '.')
        
        #=======================================================================
        # # Everything else goesinto the conf
        # for key, value in kwargs.iteritems():
        #     self.CONF.set(key, value)
        #=======================================================================        
        #=== loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
        # # Log something
        # log.debug("Running {C}.{M}...".format(C = self.app_name, M = inspect.stack()[0][3]), 
        #          app_name     = self.app_name,
        #          logfile      = self.logfile, 
        #          log_level    = self.log_level, 
        #          screendump   = self.screendump, 
        #          create_paths = self.create_paths, 
        #          )
        #=======================================================================
    
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

    @property
    def file(self):
        try:
            return self.FILE
        except (AttributeError, KeyError, ValueError) as e:
            err = "Attribute {A} is not set. ".format(A = str(stack()[0][3]))
#             log.error() # loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        
    @file.setter
    def file(self, value):
        _value = str(value)
        try: 
            FH = open(_value, "rb")
            FH.close()
        except Exception as e:
            err = "{A} = {V}. Unable to open file (ERR: {E})".format(A = str(stack()[0][3]), V = _value, E = str(e))
#             log.error(err)# loghandler disabled until bugfix in Jessie access to self.socket.send(msg)
            raise ValueError(err)
        else:
            self.FILE = _value
    
    @file.deleter
    def file(self):
        del self.FILE

    def _recreate_pickle_from_text_output(self, file):
        """
        pickle: 
            archive_path = The filename in the archive disk.

            link = The non-existant path that the archive_path was pointing to 

            found = a list containing the path of a found file, and 
                    its score (as a list) as it relates to the path of the link.

            _pickle[_path] = {"link":None, "exists":None, "found":[]}

            _pickle[archive_path] = /mnt/archiveDisk/dir1/dir2/filename    
            _pickle[archive_path]["link"] = /linkdir1/linkdir2/filename
            _pickle[archive_path]["exists"] = None
            _pickle[archive_path]["found"]= [
                                    [/wrongdir1/wrongdir2/filename,[-1, -1, 0]], 
                                    [/wrongdir1/rightdir2/filename,[-1, 0, 0]], 
                                    [/rightdir1/rightdir2/filename,[0, 0, 0]], 
                                    etc.
                                    ]
        """
        def _patherror():
            err = """The dict key 'path' is not set. 
            This should always come before the 'link' line or a 'path:score' line, so something is out of whack. 
            Halting the process. 
            _path = '{P}'
            link = '{L}'
            linecount = {LC}
            """.format(P = str(_path), L = line, LC = str(linecount))
            raise RuntimeError(err)
            
        def _setpath(line):
            _path = line.split(":", 1)[1]
            _path = _path.strip()
            if not _path.startswith(_delim): _path = _delim + _path
            # Create dict key with list
            try:
                _pickle[_path]
                print("!!Overwriting existing key: '{K}'!!".format(K = _path))
            except KeyError as e:
                pass
            
            _pickle[_path] = {"link":None, "exists":None, "found":[]}
            
            return _path
            
        def _add_link(line):
            if _path is None: _patherror() 
            _pickle[_path]["link"] = line.split(":", 1)[1].strip()
            

        def _add_scored(line):
            if _path is None: _patherror()                 
            found,score = line.split(":", 1)
            found = found.strip()
            score = score.strip()
            score = literal_eval(score)            
            _pickle[_path]["found"].append([found,score])
            
        _pickle = {}
        _path = None
        linecount = 0
        # score_pattern based on: /beegfs/(snip)/CircCoordinates : [-1, -1, -1, -1, -1, -1, -1, 0, 0]
        score_pattern = "^\s*[\./]*.*\:\s*\[[-\,01 ]*\].*$"
        with open(file) as f:
            for line in f:
                linecount += 1
                # everything must if/elif!!!
                if "Adding" in line: _path = None
                elif re.match("^=*$", line): _path = None 
                # path: mnt/ARCHIVEDISKS/(...)/Mus_Brain_RNAseR_minus_2.fastq.gz
                elif re.match("^\s*path\:.*$",line): _path = _setpath(line)
                elif re.match("^\s*linked-to\:.*$", line): _add_link(line)
                elif re.search("\!Nothing found\!", line): _path = None
                # Last!
                elif re.match(score_pattern, line): _add_scored(line)
                else:
                    continue
                
        return _pickle                        

    def evaluate_score(self, scorelist, found, disallowed_range = 2,  allowed_loops = ["/mnt/group_cd/data/"]):
        """"""
        match = 0
        mismatch = -1

        if not isinstance(scorelist, (list, tuple)):
            err = "_evaluate_score: Parameter 'scorelist' must be a list. (scorelist = {T}'{V}'".format(T = str(type(scorelist)), V = str(scorelist))
            raise ValueError(err)        
        ###########################################################
        # Scoring exception! 
        # -1s following 0s are probably not a right match, so always fail
        _scorelist = str(scorelist)
        if re.search('-1(, 0)+(, -1)+', _scorelist): return 0
        # The first two should always be not-match, accounting for /mnt/Archive_Disk-#
        test = []
        for i in range(0,disallowed_range,1):
            test.append(scorelist.pop(0))
        for allowed_loop in allowed_loops:
            if allowed_loop in found:
                test = [mismatch] # Give it a pass
        if match in test:
            err = ''.join(["'", str(scorelist), "' matches one of the first ", disallowed_range, " number of directories.\nThis should never happen...so halting.  "])
            raise RuntimeError(err)
        # Here forward, directories can match, so score them
        #####################################################################################
        # change scoring list because 0 and -1 was stupid
        newlist = []
        for i in scorelist:
            if i == -1: newlist.append(0)
            elif i == 0: newlist.append(1)
            else:
                err = "You have an invalid number in scorelist ({V})".format(str(scorelist))
                raise ValueError(err)
        scorelist = newlist     
        #####################################################################################
        _length = len(scorelist)
        _score = 0
        
        for i in scorelist: _score += i/_length
        return _score

    def copy_found(self, copy_to_dir, file):
        _file = str(file)
        try:
            _src_size = os.stat(_file).st_size
        except FileNotFoundError as e:
            err = "Unable to determine file statistics for '{F}'. !SKIPPING!".format(F = _file)
            print(err)
            return False
        
        _dstname = copy_to_dir + _delim + "COPIED" + '-'.join(_file.split("/"))
        msg = "Copying '{F}'\n to \n'{D}'...".format(F = _file, D = _dstname)
        
        try: 
#             print("fake copy src:{F}, dst:{D}".format(F = _file, D = _dstname))
            copy2(_file, _dstname)
            msg += "OK"
        except Exception as e:
            msg += "!FAILED! (ERR: {E})".format(str(e))
        print(msg)
        
        _dst_size = os.stat(_dstname).st_size
        if _dst_size !=_src_size:
            err = "Copied file destination size {DS} does not match source file site {SS}. !Please confirm!".format(DS = str(_dst_size), SS = str(_src_size)) 
            print(err)
        
        return True
                
    def main(self):
        """"""
        print("in main()")
        pickle = self._recreate_pickle_from_text_output(self.file)
        _pickle = dict(pickle)
        
        for key, value in _pickle.items():
#             print("==================")
#             print(key)
#             print("-->")
#             print(pickle[key]["link"])
            key_dir  = ntpath.dirname(key)
            key_file = ntpath.basename(key)
            _found_list = pickle[key]["found"]
            for _list in _found_list:
                _found_path  = _list[0]
                _found_score = _list[1]
#                 print(_found_path, "--->", _found_score)
                _evaluation = self.evaluate_score(_found_score, _found_path)
                if _evaluation > 0.98:
                    msg = ''.join([key, "\n-->\n",
                                   pickle[key]["link"], "\n",   
                                   "was evaluated aginst \n",
                                   _found_path, "(", str(_found_score), ")", 
                                   "\nwith a score of ", str(_evaluation), ".\n", 
                                   "COPYING..."
                                   ])
                    print(); print(msg)
                    if self.copy_found(key_dir, _found_path):
                        pickle.pop(key, None) # Remove it from the pickle object
        
        self._pickleit(pickle)
                        
                
              
            #===================================================================
            # copiedsrc = lines[1].rstrip("\n").rstrip("\r")
            # copieddst = lines[3].rstrip("\n").rstrip("\r")
            # replacelink = lines[5].rstrip("\n").rstrip("\r")
            # sizefrom, sizeto = lines[6].split(" to ")
            # linkrootdir = ntpath.dirname(replacelink)
            # copied_dst_src_path = linkrootdir + "/" + "COPY-OF" + '-'.join(copiedsrc.split("/"))
            # print("linkrootdir:", linkrootdir)
            # print("copiedsrc:", copiedsrc)
            # print("copieddst:", copieddst)
            # print("replacelink=", replacelink)
            # print("sizefrom:", sizefrom) 
            # print("sizeto:", sizeto)
            # print("copiedsrc:", copiedsrc)
            # print("copied_dst_src_path:", copied_dst_src_path)
            # import sys
            #===================================================================
                
            #===================================================================
            #     if  not checks.pathExists(copieddst):
            #         err = "Unable to verify file '{F}'. Aborting this segment."
            #         print(err.format(F = copieddst))
            #         continue
            #     try: _result = subprocess.check_output(["ls", "-lad", replacelink])
            #     except subprocess.CalledProcessError as e:
            #        err = "Unable to verify file '{F}' due to a Non-zero status from 'ls'. Aborting this segment."
            #        print(err.format(F = replacelink))
            #        print(str(e))
            #        continue
            #     print("_result=", _result)
            #     if not len(copied_dst_src_path) > len(copiedsrc):
            #         err = """Somethings wrong with the #copied_dst_src_path# filenmae basd on the copied file path...
            #                  CDSP: {C}
            #                  Copied path: {P}
            #                  Aborting this segment.""".format(CDSP = copied_dst_src_path, P = copiedsrc)
            #         print(err)
            #         continue
            #     
            #     print()
            #     print("Correcting link: '{L}' to data copied from '{F}' as '{C}'".format(L = replacelink, F = copiedsrc, C = copied_dst_src_path))
            #     # First rename the wrongly named file. Halt if fails
            #     if not self._runcommand(["mv", copieddst, copied_dst_src_path]): continue
            #     # The remove the link. Its OK if this fails. 
            #     if not self._runcommand(["rm", "-rf", replacelink]): pass
            #     if not self._runcommand(["ln", "-sf", copied_dst_src_path, replacelink]):
            #         err = "LINK NOT REPLACED. Manually replace with 'ln -sf {F}  {L}".format(F = copied_dst_src_path, L = replacelink)
            #         print(err)
            # 
            # except ValueError as e:
            #     print("Done.")
            #===================================================================
                      

        
    
if __name__ == '__main__':
    parser = ArgumentParser()
    object = CopyFoundFolders(parser)
