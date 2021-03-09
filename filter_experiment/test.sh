#!/bin/bash

########## point query ##########
# ../build/filter_experiment/filter_experiment no_filter_data 0 1 0 0 0 0
# ../build/filter_experiment/filter_experiment bloom_data     1 1 0 0 0 0
# ../build/filter_experiment/filter_experiment surf_data      2 1 0 0 0 0
../build/filter_experiment/filter_experiment rosetta_data   5 1 0 0 0 0

########## range query ##########
# ../build/filter_experiment/filter_experiment no_filter_data 0 1 0 2 0 0
# ../build/filter_experiment/filter_experiment bloom_data     1 1 0 2 0 0
# ../build/filter_experiment/filter_experiment surf_data      2 1 0 2 0 0
# ../build/filter_experiment/filter_experiment rosetta_data   5 1 0 2 0 0
