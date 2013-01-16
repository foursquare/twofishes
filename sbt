#!/bin/sh
java -Xms6g -Xmx12g -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar "$@"
