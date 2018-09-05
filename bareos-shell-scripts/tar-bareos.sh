#!/bin/bash

if [ -z ${1} ]; then
     SRC="/etc/bareos"
else
     SRC=$(echo "$1" | cut -f1 -d";") # Simple sanitize
fi

if [ -z ${2} ]; then 
    DST="/prj/bareos_iventory/etc-bareos.tar"
else
     DST=$(echo "$2" | cut -f1 -d";") # Simple sanitize
fi

echo "SRC=$SRC, DST=$DST"

cd $SRC
tar -cf $DST ./*
