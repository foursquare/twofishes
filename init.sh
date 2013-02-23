#!/bin/sh
if [ ! -f sbt-launch.jar ];
then
  curl -o sbt-launch.jar http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.12.2/sbt-launch.jar
fi
