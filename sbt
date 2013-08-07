#!/bin/sh
export JAVA_TOOL_OPTIONS="-Xmx6G"
java -Dsbt.ivy.home=$HOME/.twofishivyspindle  -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar  "$@"
