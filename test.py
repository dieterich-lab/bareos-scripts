from common.loghandler import log
from datetime import datetime
from common.convert_timestring_input import convert_timestring_input as human_time 
from common.checks         import Checks
checks = Checks()

import os
import time

startdir = "/Users/mikes/Documents/Work/Heidelberg/projects/BareOS/bareos-virtualenv/bareos-scripts"
maxdepth = 3
results = []
older = 0
newer = 0
FILETYPE = 'd'

_now = time.time()
for root, dirs, files in os.walk(startdir):
    dir_youngest_time = 0 # Reset at each root loop
    # Check depth RELATIVE TO root
    dir_depth = root.replace(startdir, "") # Remove relative root
    dir_depth = dir_depth.split(checks.directory_deliminator())
    dir_depth = [x for x in dir_depth if len(x) > 1]
    dir_depth = len(dir_depth) + 1 # Start at 1 not 0
    if dir_depth <= maxdepth: 
        for fn in files:
            path = os.path.join(root, fn)
            _time = os.stat(path).st_mtime # in epoch
            _diff = _now - _time
#             print("_diff=", _diff) #333
            if ((_diff >= older) or (older == 0)) and ((_diff <= newer) or (newer == 0)):
                 if _diff < dir_youngest_time:  dir_youngest_time = _diff
                 if ("f" in FILETYPE.lower()) or ("b" in FILETYPE.lower()):
                     results.append([path, human_time(int(_diff), "h"), "hours"])  
                     print(path, ":", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(_time)), human_time(int(_diff), "h"),"hours")
                     print("dir_youngest_time=", dir_youngest_time)
                     
        if "d" in FILETYPE.lower():
            results.append([root, human_time(int(dir_youngest_time), "h"), "hours"])  
            print(path, ":", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(_time)), human_time(int(_diff), "h"), "hours")

    else: 
        print("dir_depth = {D}..Skipping".format(D=dir_depth))
        
print(results)
# if __name__ == "__main__":
    