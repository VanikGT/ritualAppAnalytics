package com.ritual.ritualapp

import org.apache.spark.SparkConf


object Settings {
  val appName: String = "Moxie Logcat Parser"
  val sparkConf: SparkConf = new SparkConf()
    .set("spark.logConf", "true")
    .set("spark.cleaner.periodicGC.interval", "5m") // default is 30m
    .set("spark.sql.sources.partitionOverwriteMode","dynamic") // overwrite at a partition level
    .set("spark.network.timeout", "360000") // default is 120000 (120s) and GC might be slow on heavy days causing timouts
    .set("spark.sql.codegen.wholeStage", "false") // disables whole stage code gen (above 64k code)
    //    .set("spark.sql.optimizer.maxIterations", "300") // sometimes we get a warning: org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$2: Max iterations (100) reached for batch Operator Optimization before Inferring Filters, please set 'spark.sql.optimizer.maxIterations' to a larger value.
    .set("spark.sql.ansi.enabled", "true") // we want F.size(null) to be null instead of -1
    .set("spark.sql.debug.maxToStringFields", "75") // defaults to 25. increase to avoid warnings in the log
    .set("spark.hadoop.fs.gs.implicit.dir.repair.enable", "false") // Yonatan suggested this, from https://groups.google.com/g/cloud-dataproc-discuss/c/JKcimdnskJc?pli=1
}
