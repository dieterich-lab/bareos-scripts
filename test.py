from common.loghandler import log
from datetime import datetime
from common.convert_timestring_input import convert_timestring_input as human_time 
from common.checks         import Checks
checks = Checks()

import os
import time

startdir = "/Users/mikes/Documents/Work/Heidelberg/projects/BareOS/bareos-virtualenv/bareos-scripts"
maxdepth = 0
results = []
older = 0
newer = 0
FILETYPE = 'b'
_outfile = "STDOUT"
INCREMENTOUT = "h"
TERMINAL = True
_outfile = open("./deleteme.out", "w")

_now = time.time()
for root, dirs, files in os.walk(startdir):
    # Check depth RELATIVE TO root
    dir_depth = root.replace(startdir, "") # Remove relative root
    dir_depth = dir_depth.split(checks.directory_deliminator())
    dir_depth = [x for x in dir_depth if len(x) > 1]
    dir_depth = len(dir_depth) + 1 # Start at 1 not 0

    if (dir_depth <= maxdepth) or (maxdepth == 0):
        _dir_youngest_time = os.stat(root).st_mtime # Reset at each root loop

        for fn in files:
            path = os.path.join(root, fn)
            _time = os.stat(path).st_mtime # in epoch
            _diff = _now - _time
            # Set the directory time to the youngest file in the dir
            if _time > _dir_youngest_time: _dir_youngest_time = _time
            # If it matches the input time rnge
            if ((_diff >= older) or (older == 0)) and ((_diff <= newer) or (newer == 0)):
                 # If individual file listings was set
                 if ("f" in FILETYPE.lower()) or ("b" in FILETYPE.lower()):
                     _append = [path, human_time(int(_diff), INCREMENTOUT), INCREMENTOUT]
                     results.append(_append)  
                     if TERMINAL: print(_append)
                     if _outfile is not None: _outfile.write(str(_append))
        # If directorys was set
        if ("d" in FILETYPE.lower()) or ("b" in FILETYPE.lower()):
            _diff = _now - _dir_youngest_time
            _append = [root, human_time(int(_diff), INCREMENTOUT), INCREMENTOUT]
            results.append(_append)  
            if TERMINAL: print(_append)
            if _outfile is not None: _outfile.write(str(_append))

#     else: 
#         print("dir_depth = {D}..Skipping".format(D=dir_depth))

_outfile.close()        
print(results)
# if __name__ == "__main__":
    