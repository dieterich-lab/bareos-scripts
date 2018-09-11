#!/bin/bash

scriptname=`basename "$0"`

### USAGE ######################################################################
read -r -d '' USAGE << ENDOFUSAGE
${scriptname}  -i -L -T -v

    This script deletes (from the Bareos Database) any backups
    with 0 files AND 0 bytes AND which terminated with either A, T or W
        A     Canceled by the user
        T     Terminated normally
        W     Terminated with warnings

    FULL BACKUPS are always ignored. 

    -h | --help
        Print this help statement.

    -i | --interactive
        This will ask for a confirmation of each delete.
        (DEFAULT: "false")

    -L | --logfile
        The FULL path to the log output. (DEFAULT: ./${scriptname}.<date>.log)

    -T | --test
        Take no real action.

    -v | --verbose
        Verbose output.


ENDOFUSAGE
### USAGE ######################################################################

#--- SET GETOPTS --------------------------------------------------------------
OPTS=`getopt -o hiL:Tv --longoptions help,interactive,logfile,test,verbose: -n 'parse-options' -- "$@"`
if [ $? != 0 ] ; then
    echo "Failed parsing options." >&2
    echo "$USAGE" >&2
    exit 1
fi
# echo "$OPTS"
eval set -- "$OPTS"

#--- SET DEFAULTS FIRST --------------------------------------------------------
INTERACTIVE=false
DRY_RUN=false # Parameter T or  --test
VERBOSE=false
LOGFILE=""          # Must be null

#--- RUN GETOPTS --------------------------------------------------------------
while true; do
  case "$1" in
      -h | --help )                 echo "$USAGE" >&2; exit 0;     shift ;;
      -i | --interactive )          INTERACTIVE=true; 			   shift ;;
      -L | --logfile )              LOGFILE="$2"; 			shift; shift ;;
      -T | --test )                 DRY_RUN=true; 			       shift ;;
      -v | --verbose )              VERBOSE=true;                  shift ;;
      -- )                                                  shift; break ;;
      * )                                                           break ;;
  esac
done

exec 2> /dev/null # Redirect error out, to avoid errors while parsing input

#--- FUNCTIONS --------------------------------------------------------------
# None

#--- PARAM MODS And checks #-------------------------------------------------
# LOGFILE should always come first
NOW=$(date +%y%m%d.%H%M%S)
if [ ${#LOGFILE} -lt 1 ]; then  LOGFILE="${scriptname}.${NOW}.log"
elif [[ ! $LOGFILE =~ .*/.* ]]; then  LOGFILE="${LOGFILE}"
fi
# And redirect for logfile. Any echo to >1 also goes to logfile
# exec > >(tee -a $LOGFILE) # Not everything to log for now


# MAIN() ---------------------------------------------------------------------
if [ ${VERBOSE} == true ]; then
  echo "INTERACTIVE     = ${INTERACTIVE}"
  echo "DRY_RUN         = ${DRY_RUN}"
  echo "LOGFILE         = ${LOGFILE}"
  echo "VERBOSE         = ${VERBOSE}"
fi

# Quick pause
# echo
# if [ ${VERBOSE} == true ] && [ ${INTERACTIVE} == true ]; then
#     echo -n "Pausing 5 seconds. CTRL-C to abort"
#     count=0
#     while [ ${count} -lt 5 ]; do { echo -n "."; sleep 1; ((count++)); }; done
#     echo "OK"
# fi

###############################################################################
### MAIN ######################################################################

# Bareos job result codes
# A     Canceled by the user
# T     Terminated normally
# W     Terminated with warnings
while read spacer JOBID spacer NAME spacer CLIENT spacer spacer TYPE spacer LEVEL spacer JOBFILES spacer JOBBYTES spacer STATUS spacer
do
    if [[ ${LEVEL} == *"I"* ]] && [[ $STATUS == *"C"* ]]; then

        if [ true ]; then

            # THIS IS REALLY, REALLY IMPORTANT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # If you dont strip the comma out of the JOBID, bconsole will see  !
            # it as two separate jobs!                                         !
            # I.e. will interpret "delete 4,123" as "delete 4 and delete 123"  !
            JOBID=${JOBID/,} #                                                 !
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            line="DELETING '$JOBID $NAME $CLIENT $STARTDATE $STARTTIME $TYPE $LEVEL $JOBFILES $JOBBYTES $STATUS' "

            # Interactive only
            if [ ${INTERACTIVE} == true ]; then
                response=$(echo "list jobid=${JOBID}" | bconsole | grep '^\+|\|')
                output=$( echo "${response}" | grep -P "^[\+\|].*" )
                echo "${output}"
                echo -n "Delete this job?(Tap the single key 'y' or 'n')"
                read -n 1 yn </dev/tty
                echo
                case $yn in
                    [Yy]* )
                        line="${line}(Interactive user approved)"
                        ;;
                    * )
                        echo "${line}(Interactive user canceled) ... FAILED!" | tee -a ${LOGFILE}
                        continue
                        ;;
                esac

            else
                line="${line}(Non user interactive)"
            fi

            # If here, you did not continue at the quesiton, meaning you hit "y"
            #echo -n "DELETING ... $JOBID $NAME $CLIENT $STARTDATE $STARTTIME $TYPE $LEVEL $JOBFILES $JOBBYTES $STATUS ..."
            if [ ${DRY_RUN} == true ]; then
                line="${line}(TEST, no real action taken!)"
            else
                echo "cancel jobid=${JOBID}" | bconsole
                echo "delete jobid=${JOBID}" | bconsole
            fi

            [[ $? -eq 0 ]] && line="${line} ... OK" || line="${line} ... FAILED!"
            echo "${line}" | tee -a ${LOGFILE}
        fi
    fi
done <<< "$(echo "list jobs" | bconsole )"

echo "DONE"
