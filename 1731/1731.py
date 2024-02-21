# -- 1731. The Number of Employees Which Report to Each Employee

# -- Table: Employees

# -- +-------------+----------+
# -- | Column Name | Type     |
# -- +-------------+----------+
# -- | employee_id | int      |
# -- | name        | varchar  |
# -- | reports_to  | int      |
# -- | age         | int      |
# -- +-------------+----------+
# -- employee_id is the column with unique values for this table.
# -- This table contains information about the employees and the id of the manager they report to. Some employees do not report to anyone (reports_to is null). 
 

# -- For this problem, we will consider a manager an employee who has at least 1 other employee reporting to them.

# -- Write a solution to report the ids and the names of all managers, the number of employees who report directly to them, and the average age of the reports rounded to the nearest integer.

# -- Return the result table ordered by employee_id.

# -- The result format is in the following example.


employees_df.groupBy("reports_to", "name") \
                        .agg(count("reports_to").alias("reports_count"),
                             round(avg("age"), 0).alias("average_age")) \
                        .orderBy("reports_to").show()
