#!/bin/bash

which statelint > /dev/null
if [ $? -ne 0 ] ; then
    echo "Need statelint installed on system, cannot run"
    exit 1
fi

for input in `ls *.hsd`; do
    output="${input%.*}.sfn"
    tmp="tmp.sfn"
    ../bin/heaviside compile $input -o $tmp 2> /dev/null
    if [ $? -eq 0 ] ; then
        echo "Verifying results for ${input}"
        # if there is an error, the compiler prints the error message
        diff -u $tmp $output
        if [ $? -eq 0 ] ; then
            # if there is an error, the compiler prints the error message
            statelint $output
        fi
    fi
    rm $tmp
done
