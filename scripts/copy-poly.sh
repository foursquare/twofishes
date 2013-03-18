#!/bin/sh

grep --no-filename $1 data/private/polygons/* | perl -p -e "s/$1/$2/" >> data/private/polygons/99-manual.txt
