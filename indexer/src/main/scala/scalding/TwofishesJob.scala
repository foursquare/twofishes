// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import com.twitter.scalding._

class TwofishesJob(name: String, args: Args) extends Job(args) {

  val outputBaseDir = args("output")

  protected def concatenatePaths(base: String, relative: String): String = {
    // TODO: strip trailing and leading slashes
    base + "/" + relative
  }

  val outputPath = concatenatePaths(outputBaseDir, name)

  def onSuccess(): Unit = ()

  override def run: Boolean = {
    val result = super.run
    if (result) onSuccess()
    result
  }
}