# Ritual App Analytics Documentation
Below, I'll describe the technologies I have used, the problems I've encountered, and issues related to the task.

# Aproache For Big Data(Streaming or Batch)
For streaming, Spark Streaming is a viable option. To optimize the code, we could add a checkpoint mechanism, create meaningful partitions, and incorporate a logging system, among other improvements. The choice between using a Data Lake (such as partitioned Parquet files) and a Data Warehouse depends on the intended use of the data. Additionally, incorporating a session_id in the front-end would lead to clearer and more meaningful data capture. If the budget is constrained and real-time tracking of user behavior is not a priority, batch processing could be a cost-effective alternative.
 

# Technologies:
I have utilized Spark Scala for data cleaning, addressing any missing or anomalous values. Additionally, I have performed some analytics, although all tasks could also be completed using pure SQL. There's no significant difference for me between PySpark and Spark Scala. In conclusion, I have exported the datasets to the 'out' folder as CSV files. Then, I manually uploaded them into BigQuery tables. The manual upload ensures that you can run the code yourself without any credential issues.

# Visualization
I have used Looker Studio for visualization purposes. I have connected Looker with bigquery:
```
SELECT
  *,
  LAG(event_type) OVER (
    PARTITION BY user_id
    ORDER BY event_date
  ) AS prev_event_type,
  LEAD(event_type) OVER (
    PARTITION BY user_id
    ORDER BY event_date
  ) AS next_event_type,
  DATE_DIFF(
    CAST(event_date AS DATE),
    signup_date,
    WEEK
  ) + 1 AS week_number_since_signup
FROM
  `ritual-app-404011.user_data.user_events`
JOIN
  `ritual-app-404011.user_data.user_profiles`
USING
  (user_id)
```
The remaining work was carried out in Looker. [Here is the link to Report.](https://lookerstudio.google.com/reporting/d67e77e1-1345-4b59-8c1a-4ddac5e21a28)

# Problems
There are some anomalous values, such as instances where the signup_date is later than the event_date; I have filtered these out along with any duplicates. Since there were no significant spikes or drop-offs in the data, I have included only the code for calculating these metrics without generating a corresponding chart.

The task was executed using Spark Scala, but it could also be accomplished with PySpark if needed(even with pure pandas due to the current data size). However, due to the small size of the dataset, it's challenging to investigate anomalous cases or focus on code optimization. Ideally, the output datasets should be written to a data lake or directly into database tables rather than CSV files. Additionally, the partitioning technique applied during the write process is currently hardcoded to a single partition; for larger datasets, this logic would need to be revised based on the size of the data and other factors. Overall, the task leaned more towards data analytics than data engineering.

Tehniques, suggestions and other staff we can discuss during interview!

Thank You)
