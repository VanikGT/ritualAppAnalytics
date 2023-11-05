package com.ritual.ritualapp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

import scala.collection.mutable.Map

object Analytics {

  def spikesDropOffs(spark: SparkSession, analytics:  Map[String, DataFrame]): DataFrame = {
    import spark.implicits.StringToColumn
    val userWindow = Window.partitionBy("user_id").orderBy("event_date")

    // Calculate the daily count of events for each user
    val dailyEventsDF = analytics("events")
      .groupBy($"user_id", $"event_date")
      .agg(F.count("event_id").alias("daily_events"))

    // Calculate the moving average and standard deviation
    val statsDF = dailyEventsDF
      .withColumn("moving_avg", F.avg($"daily_events").over(userWindow.rowsBetween(-7, 0))) // 7-day moving average
      .withColumn("moving_stddev", F.stddev($"daily_events").over(userWindow.rowsBetween(-7, 0))) // 7-day moving stddev

    val spikesDropOffs = statsDF
      .withColumn("is_spike",$"daily_events" > ($"moving_avg" + F.lit(3) * $"moving_stddev"))
      .withColumn("is_dropoff", $"daily_events" < ($"moving_avg" - F.lit(3) * $"moving_stddev")).persist

    spikesDropOffs.count
    analytics += ("spike_dropoffs" -> spikesDropOffs)
    spikesDropOffs
  }


  def eventAnalysis(spark: SparkSession, analytics: Map[String, DataFrame]): DataFrame = {
    import spark.implicits.StringToColumn
    val userWindow = Window.partitionBy("user_id").orderBy("event_date")

    // Use lead function to get the next event type for each event
    val leadLagDF = analytics("events")
      .withColumn("next_event_type", F.lead("event_type", 1).over(userWindow))
      .withColumn("previous_event_type", F.lag("event_type", 1).over(userWindow))
      .select($"user_id", $"event_type", $"previous_event_type", $"next_event_type")

    val viewBeforeClickDF = leadLagDF
      .filter($"event_type" === "view" && $"next_event_type" === "click")

    val viewBeforePurchaseDF = leadLagDF
      .filter($"previous_event_type" === "view" && $"event_type" === "purchase")

    val eventSequenceAnalysisDF = viewBeforeClickDF.union(viewBeforePurchaseDF).persist

    eventSequenceAnalysisDF.count
    analytics += ("event_sequence_analysis" -> eventSequenceAnalysisDF)
    eventSequenceAnalysisDF
  }


  def eventsPerWeek(spark: SparkSession, analytics: Map[String, DataFrame]): DataFrame = {
    import spark.implicits.StringToColumn
    val userWindow = Window.partitionBy("user_id").orderBy("event_date")
    val eventsSinceSignup = analytics("events")
      .join(analytics("users"), Seq("user_id"))
      .withColumn("weeks_since_signup", F.datediff($"event_date", $"signup_date") / 7)

    // Round down to get the complete weeks
    val eventsGroupedByWeek = eventsSinceSignup
      .withColumn("week_number", F.floor($"weeks_since_signup"))
      .groupBy($"user_id", $"week_number")
      .agg(F.count("*").alias("events_per_week"))
      .orderBy($"user_id", $"week_number")

    eventsGroupedByWeek.count
    analytics += ("events_grouped_by_week" -> eventsGroupedByWeek)
    eventsGroupedByWeek
  }


}
