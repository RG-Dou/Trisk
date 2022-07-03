#!/bin/bash

bash nexmark-run.sh 800 q20 false
bash nexmark-run.sh 800 q20 true

# for StateSize in 300000; do
#     for BlockCache in 03; do
  # bash stock-run.sh ${BlockCache}
#  	 bash nexmark-run-2.sh ${BlockCache} q3 ${StateSize}
#     done
#done
