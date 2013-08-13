#!/bin/sh
JAVA_TOOL_OPTIONS="-Xmx6G $JAVA_TOOL_OPTIONS" java -Dsbt.ivy.home=$HOME/.twofishivy  -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar  "$@"
