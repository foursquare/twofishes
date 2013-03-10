#!/bin/sh
export JAVA_TOOL_OPTIONS="-Xmx12G"
java  -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar  "$@"
