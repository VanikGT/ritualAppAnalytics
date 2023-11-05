version := "0.0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.scopt" %% "scopt" % "4.1.0",
//  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.29.0",

  //  "com.google.cloud" % "google-cloud-bigquery" % "2.5.0"
)

assemblyMergeStrategy in assembly := {
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resourceDirectory in Compile := baseDirectory.value / "src/main/resources"