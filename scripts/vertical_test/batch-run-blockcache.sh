#!/bin/bash

for BlockCache in 05; do
#     for BlockCache in 03 80; do
#    bash nexmark-run.sh ${BlockCache} q3window
    bash nexmark-run.sh ${BlockCache} q20
done

# for StateSize in 300000; do
#     for BlockCache in 03; do
  # bash stock-run.sh ${BlockCache}
#  	 bash nexmark-run-2.sh ${BlockCache} q3 ${StateSize}
#     done
#done
