#!/bin/bash

#for BlockCache in 01 05 10 15 20; do
for BlockCache in 80; do
  bash stock-run.sh ${BlockCache}
done
