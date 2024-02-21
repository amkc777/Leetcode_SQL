employees_df.groupBy("reports_to", "name") \
                        .agg(count("reports_to").alias("reports_count"),
                             round(avg("age"), 0).alias("average_age")) \
                        .orderBy("reports_to").show()
