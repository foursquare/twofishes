 //  Copyright 2012 Foursquare Labs Inc. All Rights Reserved
package com.foursquare.twofishes

import java.io.IOException
import scala.io.Source

case class GeocodeServerConfig(
  runHttpServer: Boolean = true,
  thriftServerPort: Int = 8080,
  host: String = "0.0.0.0",
  hfileBasePath: String = "",
  shouldPreload: Boolean = true,
  shouldWarmup: Boolean = false,
  maxTokens: Int = 10,
  reload: Boolean = false,
  hotfixBasePath: String = "",
  enablePrivateEndpoints: Boolean = false,
  minMapCount: Int = 131060
)

object GeocodeServerConfigSingleton {
  var config: GeocodeServerConfig = null

  def init(args: Array[String]) = {
    config = GeocodeServerConfigParser.parse(args)
    config
  }
}

object GeocodeServerConfigParser {
  val VmMaxMapConfig = "/proc/sys/vm/max_map_count"
  def maxMapCountOpt: Option[Int] = {
    try {
      Source.fromFile(VmMaxMapConfig).getLines.map(_.toInt).toVector.headOption
    } catch {
      case ioe: IOException => None
    }
  }

  def parse(args: Array[String]): GeocodeServerConfig = {
    val parser = new scopt.OptionParser[GeocodeServerConfig]("twofishes") {
      opt[String]("host")
        .text("bind to specified host (default 0.0.0.0)")
        .action { (x, c) => c.copy(host = x) }
      opt[Int]('p', "port")
        .action { (x, c) => c.copy(thriftServerPort = x) }
        .text("port to run thrift server on")
      opt[Boolean]('h', "run_http_server")
        .action { (x, c) => c.copy(runHttpServer = x) }
        .text("whether or not to run http/json server on port+1")
      opt[String]("hfile_basepath")
        .text("directory containing output hfile for serving")
        .action { (x, c) => c.copy(hfileBasePath = x) }
        .required
      opt[Boolean]("preload")
        .text("scan the hfiles at startup to prevent a cold start, turn off when testing")
        .action { (x, c) => c.copy(shouldPreload = x) }
      opt[Boolean]("warmup")
        .text("warmup the server at startup to prevent a cold start, turn off when testing")
        .action { (x, c) => c.copy(shouldWarmup = x) }
      opt[Int]("max_tokens")
        .action { (x, c) => c.copy(maxTokens = x) }
        .text("maximum number of tokens to allow geocoding")
      opt[String]("hotfix_basepath")
        .text("directory containing hotfix files")
        .action { (x, c) => c.copy(hotfixBasePath = x) }
      opt[Boolean]("enable_private_endpoints")
        .text("enable private endpoints on server")
        .action { (x, c) => c.copy(enablePrivateEndpoints = x)}
      opt[Int]("vm_map_count")
        .text("minimum required per-process virtual mem map count")
        .action { (x, c) => c.copy(minMapCount = x) }
      checkConfig { c =>
        maxMapCountOpt.map(maxMapCount =>
          if (c.minMapCount > maxMapCount) {
            failure (
              "Insufficient per-process virtmem areas: %d required: %d\n".format(maxMapCount, c.minMapCount) +
              "Please increase the number of per-process VMA with sudo sysctl -w vm.max_map_count=X\n" +
              "or reduce the number required by passing --vm_map_count MAP_COUNT, but expect OOMS!\n"
            )
          } else {
            success
          }
        ).getOrElse(success)
      }
    }

    // parser.parse returns Option[C]
    parser.parse(args, GeocodeServerConfig()) getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      GeocodeServerConfig()
    }
  }
}
