#!/bin/sh

thrift --gen java -o interface/src/main/java interface/src/main/thrift/geocoder.thrift
