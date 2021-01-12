#!/bin/bash
mkdir -p temp
echo $0
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE temp/$filename

