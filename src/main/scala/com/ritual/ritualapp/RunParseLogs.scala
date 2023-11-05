package com.ritual.ritualapp


import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import scala.collection.mutable.Map

object RunParseLogs {

  def main(args: Array[String]): Unit = {

    val builder = SparkSession.builder.appName(Settings.appName).master("local")
    val spark = builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //Reading and cleaning DataFrames
    val usersDF = spark.read
                        .option("header", "true")
                        .csv(this.getClass.getResource("/user_profiles.csv").getPath)
                        .withColumn("signup_date", F.to_date($"signup_date", "yyyy-MM-dd"))
                        .withColumn("last_login_date", F.to_date($"last_login_date", "yyyy-MM-dd"))
                        .filter($"user_id".isNotNull && $"user_id" =!= "")
                        .filter($"signup_date" <= F.current_date() || $"signup_date" > $"last_login_date")
                        .dropDuplicates()
                        .withColumn("engagement_duration", F.datediff($"last_login_date", $"signup_date"))
                        .persist

    val eventsUnfilteredDF = spark.read
                                  .option("header", "true")
                                  .csv(this.getClass.getResource("/user_events.csv").getPath)
                                  .withColumn("event_date", F.to_timestamp($"event_date", "yyyy-MM-dd HH:mm:ss"))
                                  .filter($"event_id".isNotNull && $"event_id" =!= "" && $"user_id".isNotNull && $"user_id" =!= "")
                                  .filter($"event_date" <= F.current_date())
                                  .dropDuplicates()
                                  .persist

    val eventsDF = eventsUnfilteredDF.join(usersDF, Seq("user_id"), "right")
                                      .filter($"event_date" > $"signup_date")
                                      .select($"event_id",
                                                    $"user_id",
                                                    $"event_type",
                                                    $"event_date",
                                                    $"page").persist
    eventsDF.count
    eventsUnfilteredDF.unpersist

    val analytics: Map[String, DataFrame] = Map("users" -> usersDF, "events" -> eventsDF)

    //computing analytics
    Analytics.spikesDropOffs(spark, analytics)
    Analytics.eventAnalysis(spark, analytics)
    Analytics.eventsPerWeek(spark, analytics)

    //writing analytics and row data as csv files
    analytics.foreach(pair => {pair._2.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"out/${pair._1}" )
      pair._2.unpersist}
    )

    spark.stop()
  }
}
