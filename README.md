# Ritual App Analytics Documentation
Below I'll describe the tecnologies I haves used, the probelems I Have found out and issues regarding to the task.

# Technologies:
I have used Spark Scala(Also cloud be PySpar if requeired, no difference for me) for data cleaning, handling any missing or anomalous values.Also I have calculated some analytics, though all the staff could be done with pure sql.
In the end I have exported the datasets into out folder as csv files. Then manually uploaded them into bigquery tables. I have uploaded manually so you can run the code yourself and don't have ant credential issues.

# Visualization
I have used Looker Studio for Visualization purposes. I have connected Looker with bigquery:
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
The remaining staff was done in Looker. [Here is the link to Report.](https://lookerstudio.google.com/reporting/d67e77e1-1345-4b59-8c1a-4ddac5e21a28)
