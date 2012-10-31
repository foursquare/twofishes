#!/bin/sh
if [ ! -f sbt-launch.jar ];
then
  curl -o sbt-launch.jar http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-tools.sbt/sbt-launch/0.11.2/sbt-launch.jar
fi
