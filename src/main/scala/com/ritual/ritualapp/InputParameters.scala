package com.ritual.ritualapp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode
import scopt.OptionParser



final case class InputParameters(
                                  checkpointPath: String = "some_checkpoint_path_later",
                                  eventsTable: String = "",
                                  profilesTable: String = "",
                                  saveMode: SaveMode = SaveMode.Append, // append/overwrite
                                  broadcastLimit: Int = 35000000, // 35M,
                                  recordsPerFile: Int = 10000000 // 10M, // 15000000, // 15M,
                                )

object InputParameters {

  def parseParameters(args: Array[String]): InputParameters = {
    parser.parse(args, InputParameters()) match {
      case Some(params: InputParameters) =>
        params
      case _ =>
        // arguments are bad, error message will have been displayed
        throw new IllegalArgumentException(s"Spark job submission has failed, invalid parameters=${args.mkString("[", ",", "]")}")
    }
  }

  val parser: OptionParser[InputParameters] = new OptionParser[InputParameters](Settings.appName) {

    head(Settings.appName)



    opt[String]('e', "eventsTable")
      .valueName("<eventsTable>")
      .action((e, params) => params.copy(eventsTable = e))
      .text("eventsTable, example events")

    opt[String]('p', "profilesTable")
      .valueName("<profilesTable>")
      .action((p, params) => params.copy(profilesTable = p))
      .text("profilesTable, example profiles")

    opt[String]('c', "checkpointPath")
      .valueName("<checkpointPath>")
      .validate(validatePath("checkpointPath"))
      .action((c, params) => params.copy(checkpointPath = c))
      .text("checkpointPath, example: gs://analytics_data_lake/checkpoint/")

    opt[String]('m', "saveMode")
      .optional()
      .valueName("<saveMode>")
      .action((m, params) => {
        m.toLowerCase match {
          case "append" => params.copy(saveMode = SaveMode.Append)
          case "overwrite" => params.copy(saveMode = SaveMode.Overwrite)
          case _ => params
        }
      })
      .text("saveMode, examples: append/overwrite. Defaults to ErrorIfExists")

    opt[Int]('l', "broadcastLimit")
      .optional()
      .valueName("<broadcastLimit>")
      .action((l, params) => params.copy(broadcastLimit = l))
      .text("limit for number of records in a broadcast dataframe, defaults to 35000000 (35M)")

    opt[Int]('r', "recordsPerFile")
      .optional()
      .valueName("<recordsPerFile>")
      .action((r, params) => params.copy(recordsPerFile = r))
      .text("number of max record per output file, defaults to 10000000 (10M)")

    /**
    val builder = OParser.builder[InputParameters]

  val parser: OParser[String, InputParameters] = {
    import builder._
    OParser.sequence(
      programName(Settings.appName),
      head(Settings.appName),
      opt[java.util.Calendar]('s', "startDate")
        .required()
        .valueName("<startDate>")
        .action((s, params) => params.copy(startDate = s.toString))
        .text("Start date to process log files"),
      opt[java.util.Calendar]('e', "endDate")
        .required()
        .valueName("<endDate>")
        .action((e, params) => params.copy(endDate = e.toString))
        .text("End date to process log files up until (exclusive)"),
      opt[String]('o', "outputBasePath")
        .required()
        .valueName("<outputBasePath>")
        .validate(validatePath("outputBasePath"))
        .action((o, params) => params.copy(outputBasePath = o))
        .text("outputBasePath, example: gs://analytics_data_lake/temp/poc2.2/"),
      opt[Int]('r', "recordsPerFile")
        .optional()
        .valueName("<recordsPerFile>")
        .action((r, params) => params.copy(recordsPerFile = r))
        .text("number of max record per output file, defaults to 10000000 (10M)"),
      opt[Unit]("useDatabricks")
        .valueName("<useDatabricks>")
        .action((_, params) => params.copy(useDatabricks = true))
        .text("useDatabricks is a flag to set when running with databricks cluster")
    )
     */
    def validateDate(paramName: String): String => Either[String, Unit] =
      date => {
        val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")

        try {
          sdf.parse(date)
          success
        } catch {
          case _: Throwable =>
            failure(
              s"Option --$paramName must be a valid ISO date (ex: 2021-09-01)"
            )
        }
      }

    def validatePath(paramName: String): String => Either[String, Unit] =
      path =>
        if (StringUtils.isEmpty(path))
          failure(
            s"Option --$paramName must be a valid GS (gs://.../) or file system dir path"
          )
        else if (path.startsWith("gs://")) success
        else if (new java.io.File(path).isDirectory) success
        else
          failure(
            s"Option --$paramName must be a valid GS (gs://.../) or file system dir path"
          )
  }

}

