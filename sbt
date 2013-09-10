#!/bin/sh
echo $JAVA_HOME
if [ -z $JAVA_HOME ]
then
  echo using java from the search path
  JAVA_BINARY=java
else
  JAVA_BINARY=$JAVA_HOME/bin/java
  echo running java at $JAVA_BINARY
fi


JAVA_TOOL_OPTIONS="-Xmx6G $JAVA_TOOL_OPTIONS" $JAVA_BINARY -Dsbt.ivy.home=$HOME/.twofishivy  -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch.jar  "$@"
