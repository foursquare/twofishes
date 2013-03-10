#!/bin/sh
export JAVA_TOOL_OPTIONS="-Xmx4G"
java  -Xms6g -Xmx10g -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar  "$@"
