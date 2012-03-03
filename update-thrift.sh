#!/bin/sh

thrift --gen java -o server/src/main/java server/src/main/thrift/geocoder.thrift
