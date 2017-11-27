package com.matthew.localSpark

import org.kohsuke.args4j.Option

object budgetAnalyserOpts {

  @Option(name = "--csv1", usage = "csv1 location", required = true)
  var csvPath1: String =""

  @Option(name = "--csv2", usage = "csv2 location", required = false)
  var csvPath2: String =""

  @Option(name = "--props", usage = "property file location", required = false)
  var propertiesFile: String ="E:/Users/Matt/workspace/sparkPlayground/src/resources/application.conf"

  @Option(name = "--v", usage = "display verbose", required = false)
  var display: Boolean = false

  @Option(name = "--w", usage = "write to disk", required = false)
  var writeToDisk: Boolean = false

  @Option(name = "--wl", usage = "write location", required = false, depends=Array("--w"))
  var writePath: String =""
}

