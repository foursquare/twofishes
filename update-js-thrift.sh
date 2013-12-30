#!/bin/sh

thrift --gen js -o server/src/main/resources/static/ interface/src/main/thrift/geocoder.thrift 
