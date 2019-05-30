#!/bin/sh

coverage erase

coverage2 run -a -m unittest heaviside.tests
coverage3 run -a -m unittest heaviside.tests

for input in `ls tests/*.hsd`; do
    coverage2 run -a bin/heaviside -r '' -a '' compile $input -o tmp.sfn
    coverage3 run -a bin/heaviside -r '' -a '' compile $input -o tmp.sfn
done
if [ -f tmp.sfn ] ; then
    rm tmp.sfn
fi

coverage annotate --include=heaviside/*,bin/* -d tmp
coverage report --include=heaviside/*,bin/*
