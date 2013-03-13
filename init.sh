#!/bin/sh
if [ -e sbt-launch.jar ]; then
  rm sbt-launch.jar
fi
curl -o sbt-launch.jar http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.12.2/sbt-launch.jar
