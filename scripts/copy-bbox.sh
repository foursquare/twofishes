#!/bin/sh

grep --no-filename $1 data/computed/polygons/* | perl -p -e "s/$1/$2/" >> data/computed/polygons/99-manual.txt
