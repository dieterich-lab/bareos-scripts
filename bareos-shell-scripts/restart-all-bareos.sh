#!/bin/bash
# 2018-06-05 Mike Rightmire

SCRIPTNAME=`basename "$0"`
### USAGE ##############################################################
read -r -d '' USAGE << ENDOFUSAGE

${SCRIPTNAME}

-d | --dry-run
        Take no real action.
        DEFAULT: False

-h | --help
        Print this help statement.

-L | --logfile
        The path to the log output. (DEFAULT: ./bam-calls.<date>.log)
        DEFAULT: NONE

-n | --non-interactive
		Run automatically accepting no user input. 
		Will fail if input is needed. 
		Can be used in conjunction with "-w" to wait.
		DEFAULT: False

-v |--verbose
        Verbose output.

-w |--wait
        Will wait for certain obstacles to disappear (I.e. if a job is 
        running, "-w" will tell the script to wait until no jobs are 
        running and then proceed.
		DEFAULT: False

ENDOFUSAGE
### END OF USAGE ##############################################################

#--- SET GETOPTS --------------------------------------------------------------
OPTS=`getopt -o dhL:nvw --longoptions dry-run,help,logfile,non-interactive,wait,verbose: -n 'parse-options' -- "$@"`
if [ $? != 0 ] ; then
    echo "Failed parsing options." >&2
    echo "$USAGE" >&2
    exit 1
fi

#echo "OPTS = $OPTS"
eval set -- "$OPTS"

#--- SET DEFAULTS ------------------------------------------------------
DRY_RUN=false
LOGFILE=""
INTERACT=true
VERBOSE=false
WAIT=false
WAITINTERVAL=300

#--- RUN GETOPTS -------------------------------------------------------
while true; do
	case "$1" in
    	-d | --dry-run ) 			DRY_RUN=true; 			shift ;;
    	-h | --help ) 			  	echo "$USAGE" >&2;		shift ;;
		-L | --logfile ) 			LOGFILE="$2"; 			shift; shift ;;
    	-n | --non-interactive )  	INTERACT=false; 		shift ;;
		-v | --verbose ) 		  	VERBOSE=true; 			shift ;;
		-w | --wait ) 		  		WAIT=true; 				shift ;;
    	-- ) 												shift; break ;;
    	* ) 												break ;;
  esac
done

#--- PARAM MODS --------------------------------------------------------
#LOGFILE should always come first
NOW=$(date +%y%m%d.%H%M%S)
# === LOGFILE ==========================================================
if [ ${#LOGFILE} -lt 1 ]; then # NOT SET
  LOGFILE="./${SCRIPTNAME}.${NOW}.log"

elif [[ ! $LOGFILE =~ .*/.* ]]; then # SET, but NOT a Full path
  LOGFILE="${LOGFILE}"

# else # No else, just go with whats set
fi
# And redirect for logfile
exec > >(tee -a $LOGFILE)
echo "Starting log (${NOW})" >> ${LOGFILE}
# ====================================================================== 


#--- FUNCTIONS ---------------------------------------------------------
function check_job_running { # If ANY job is running
	count=0
	[[ ${VERBOSE} = "true" ]] && { echo -n "Checking for running jobs..." >&2; }
	echo "list jobs" | bconsole | while read line; do
		[[ $((${count} % 100)) -eq 0 ]] && [[ ${VERBOSE} = "true" ]] && echo -n "." >&2
		# 19 = The backup jobs result code. "R" = running
		var=$(echo ${line} | awk '{ print $19 }') 
		# [[ ${VERBOSE} ]] && echo ${line} >&2 # screen out
		[[ ${var} = "R" ]] && { echo "true"; return 0; } # return text 
		((count++))
    done
    echo "" >&2 # Line feed the screen
}

jobrunning=$(check_job_running)
while [ ${jobrunning} = "true" ]; do
	if [ ${WAIT} = "true" ]; then
		[[ ${VERBOSE} = "true" ]] && echo "waiting ${WAITINTERVAL} seconds..." >&2
		sleep ${WAITINTERVAL}
		jobrunning=$(check_job_running) 
		continue
	
	elif [ ${INTERACT} = "true" ]; then
			read -p "Job is running! Do you wish to restart anyway?" yn
			if [[ $yn = *"Y"* ]] || [[ $yn = *"y"* ]]; then
				break # end loop
			else
				exit 1 # End script without restarting
			fi

	else # Job is running, wait is false, interact is false = just end
		exit 1
		# bash needs an else here to properly recognize the 
		# nested if/then. Go figure :P
	fi
done
 

# MAIN() ---------------------------------------------------------------
service bareos-fd restart; systemctl status bareos-fd.service | tee -a &{LOGFILE}
service bareos-sd restart; systemctl status bareos-storage.service | tee -a &{LOGFILE}
service bareos-dir restart; systemctl status bareos-director.service | tee -a &{LOGFILE}
echo ""
exit 1

