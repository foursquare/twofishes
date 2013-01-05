#!/bin/sh

thrift --gen java -o interface/src/main/java interface/src/main/thrift/geocoder.thrift

if thrift -version | grep -v 0.5.0 > /dev/null; then
  echo "You need thrift compiler version 0.5.0, with finagle extensions"
  echo "see: https://github.com/mariusaeriksen/thrift-0.5.0-finagle"
  exit 1
fi

if ! grep finagle `which thrift` > /dev/null; then
  echo "You don't have thrift with finagle"
  echo "see: https://github.com/mariusaeriksen/thrift-0.5.0-finagle"
  exit 1
fi


thrift --gen java -o interface/src/main/java interface/src/main/thrift/geocoder.thrift
