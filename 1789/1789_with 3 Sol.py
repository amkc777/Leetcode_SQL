#Sol1
employee.groupBy("employee_id", "department_id") \
                    .agg(count("*").alias("count"),
                         when(col("primary_flag") == "Y", 1).otherwise(0).alias("primary_count")) \
                    .filter((col("primary_count") == 1) | (col("count") == 1)) \
                    .select("employee_id", "department_id")


#sol2
employee.filter(col("primary_flag") == "Y").select("employee_id", "department_id")\
  .union(employee.groupBy("employee_id", "department_id").count().\
       filter(col("count") == 1).select("employee_id", "department_id")).show()

#sol3
employee.select("employee_id", "department_id").where((col("employee_id")\
              .isin(employee.groupBy("employee_id").agg(count("*").alias("count"))\
                    .filter(col("count") == 1).select("employee_id")\
                        .rdd.map(lambda x: x[0]))) | (col("primary_flag") == "Y"))\
                            .distinct().orderBy("employee_id").show()

