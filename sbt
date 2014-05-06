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

JAR_PATH=`dirname $0`/sbt-launch.jar 
if [ ! -f $JAR_PATH ]; then
  echo "sbt-launch.jar missing, downloading now"
  curl -o sbt-launch.jar http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.12.2/sbt-launch.jar
fi

JAVA_TOOL_OPTIONS="-Xmx6G $JAVA_TOOL_OPTIONS" $JAVA_BINARY -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar $JAR_PATH "$@"
