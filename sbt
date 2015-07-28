#!/bin/bash

echo $JAVA_HOME
if [ -z $JAVA_HOME ]
then
  echo using java from the search path
  JAVA_BINARY=java
else
  JAVA_BINARY=$JAVA_HOME/bin/java
  echo running java at $JAVA_BINARY
fi

$JAVA_BINARY -version 2>&1 | grep -q '1.8'

if [ $? == "0" ]; then
  echo "You are running java 1.8, scala 2.10 requires java 1.7"
  if [ "$(uname)" == "Darwin" ]; then
    echo "On mac, looking for 1.7"
    JAVA_17=$(ls -d -1 /Library/Java/JavaVirtualMachines/** | grep 1.7)
    if [ $JAVA_17 ]; then
      echo "found at $JAVA_17"
      JAVA_HOME=$JAVA_17/Contents/Home/
      JAVA_BINARY=$JAVA_HOME/bin/java
    else
      echo "couldn't find java 1.7, please install"
      exit 1
    fi
  else
    exit 1
  fi
fi

JAR_PATH=`dirname $0`/sbt-launch.jar
if [ ! -f $JAR_PATH ]; then
  echo "sbt-launch.jar missing, downloading now"
  curl --location -o sbt-launch.jar http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.1/sbt-launch.jar
fi

publishTo=$publishTo JAVA_TOOL_OPTIONS="-Xmx6G $JAVA_TOOL_OPTIONS" $JAVA_BINARY  -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar $JAR_PATH "$@"
