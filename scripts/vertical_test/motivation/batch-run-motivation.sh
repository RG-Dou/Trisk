#!/bin/bash

bash nexmark-run.sh 1000 q20 true
bash nexmark-run.sh 1000 q20 false

# for StateSize in 300000; do
#     for BlockCache in 03; do
  # bash stock-run.sh ${BlockCache}
#  	 bash nexmark-run-2.sh ${BlockCache} q3 ${StateSize}
#     done
#done
