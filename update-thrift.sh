#!/bin/sh

thrift --gen java -o src/main/java src/main/thrift/geocoder.thrift
