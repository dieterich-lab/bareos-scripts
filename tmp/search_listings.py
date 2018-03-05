#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import ntpath

class SearchListings():
    def __init__(self):
        self._archive_listings = []
        for file in os.listdir("/beegfs/prj/archive_disks"):
            _path = "/beegfs/prj/archive_disks/" + file
            try:
                print("Adding " + _path )
                with open(_path, "rb") as fh:
                    for line in fh: 
                        self._archive_listings.append(line.decode("utf-8").strip())
    #                     for line in self._archive_listings: print(line) #333
            except (FileNotFoundError, PermissionError) as e:
                err = "Unable to open '{P}'. Skipping. ".format(P = _path)
                print(err)
                    
        self.search("/fu/bar/DCC.mate1")
        
    def search(self, path):
        _link_path = str(path).strip()
        all_found    = [] # results of all finds
        # Search listings first
        _link_dir  = ntpath.dirname(_link_path)
        _link_file = ntpath.basename(_link_path)
        for _archive_path in self._archive_listings:
            _archive_path = _archive_path
#             print("(SL)", end="") #33333
#             _archive_dir  = ntpath.dirname(_archive_path)
            _archive_dir  = ntpath.dirname(_archive_path)
            _archive_file = ntpath.basename(_archive_path)
#             if _link_file in _archive_path:
            if _archive_path.endswith(_link_file):
                print("'" + _link_file + "'", "is in ", "'" + _archive_path + "'")
#                 print("!!!!!!!!!!!!!!MATCH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!") 
                all_found.append(_archive_path)
        return
        
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
        
if __name__ == '__main__':
    o = SearchListings()
                    