
# importing all modules
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#start session
spark = SparkSession.builder\
        .appName("BD_challenge")\
        .getOrCreate()


# employee data
employeeColumn = ["emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date"]
employeeData = [
    ["10001","1953-09-02","Georgi","Facello","M","1986-06-26"],
    ["10002","1964-06-02","Bezalel","Simmel","F","1985-11-21"],
    ["10003","1959-12-03","Parto","Bamford","M","1986-08-28"],
    ["10004","1954-05-01","Chirstian","Koblick","M","1986-12-01"],
    ["10005","1955-01-21","Kyoichi","Maliniak","M","1989-09-12"]
] 

# job data
jobColumn = ["emp_no", "title", "from_date" , "to_date"] 
jobData = [
    ["10001","Senior Engineer","1986-06-26","9999-01-01"],
    ["10002","Staff","1996-08-03","9999-01-01"],
    ["10003","Senior Engineer","1995-12-03","9999-01-01"],
    ["10004","Senior Engineer","1995-12-01","9999-01-01"],
    ["10005","Senior Staff","1996-09-12","9999-01-01"]
] 

# salary data
salaryColumn = ["emp_no", "salary", "from_date" , "to_date"] 
salaryData = [
    ["10001","66074","1988-06-25","1989-06-25"], 
    ["10001","62102","1987-06-26","1988-06-25"],
    ["10001","60117","1986-06-26","1987-06-26"], 
    ["10002","72527","2001-08-02","9999-01-01"],
    ["10002","71963","2000-08-02","2001-08-02"], 
    ["10002","69366","1999-08-03","2000-08-02"],
    ["10003","43311","2001-12-01","9999-01-01"], 
    ["10003","43699","2000-12-01","2001-12-01"],
    ["10003","43478","1999-12-02","2000-12-01"],
    ["10004","74057","2001-11-27","9999-01-01"], 
    ["10004","70698","2000-11-27","2001-11-27"],
    ["10004","69722","1999-11-28","2000-11-27"],
    ["10005","94692","2001-09-09","9999-01-01"], 
    ["10005","91453","2000-09-09","2001-09-09"],
    ["10005","90531","1999-09-10","2000-09-09"]
] 

# 1) Create 3 data frames with the above data:

employee_df = spark.createDataFrame(employeeData,employeeColumn)
job_df = spark.createDataFrame(jobData,jobColumn)
salary_df = spark.createDataFrame(salaryData,salaryColumn)

# 2) Rename the columns by using using capital letters and replace '_' with space: 

for column in employee_df.columns:
    employee_df = employee_df.withColumnRenamed(column,column.replace("_"," ")).withColumnRenamed(column, column.upper())
print(employee_df.columns)

for column in job_df.columns:
    job_df = job_df.withColumnRenamed(column,column.replace("_"," ")).withColumnRenamed(column, column.upper())
print(job_df.columns)
    
for column in salary_df.columns:
    salary_df = salary_df.withColumnRenamed(column,column.replace("_"," ")).withColumnRenamed(column, column.upper())
print(salary_df.columns)

# 3) Format birth_date as 01.Jan.2021 
employee_df = employee_df.withColumn("birth date",F.date_format(F.col("birth date"),"DD.MMM.YYYY"))
employee_df.show()

# 4) Add a new column in employeeData where you compute the company email address by the following rule: [first 2 letter of first_name][last_name]@company.com


employee_df = employee_df.withColumn("company_email", concat_ws(substring("first name"),0,2),"last name",lit("@company.com"))
employee_df.show()


# Creating intermediate df to solve 5) and 6)

join_df = salary_df.alias("salary").join(other=job_df.alias("job"), on="emp no", how="inner")
join_df = join_df.select("emp no","title", "salary.`salary`","salary.`from date`","salary.`to date`")

condition = (
    F.when(F.col("to date")=="9999-01-01",F.ceil(F.datediff(F.current_date(),F.col("salary.`from date`"))/365))
    .otherwise(F.ceil(F.datediff(F.col("salary.`to date`"),F.col("salary.`from date`"))/365))
)

join_df = join_df.withColumn("years", condition)

window_employee = Window.partitionBy("emp no")
window_role = Window.partitionBy("title")
flag = (
    F.col("total_employee")/F.col("years_employee") < F.col("total_role")/F.col("years_role")
)

join_df = (
    join_df
    .withColumn("total_employee", F.sum(F.col("salary")*F.col("years")).over(window_employee))
    .withColumn("total_role", F.sum(F.col("salary")*F.col("years")).over(window_role))
    .withColumn("years_employee", F.sum("years").over(window_employee))
    .withColumn("years_role", F.sum("years").over(window_role))
    .withColumn("flag", F.when(flag,True).otherwise(False))
)

join_df.show()


# 5) Calculate the average salary for each job role

df = join_df.groupBy("title").agg(F.sum(F.col("salary")*F.col("years")).alias("total_salary"),F.sum(F.col("years")).alias("total_years"))

df = df.withColumn("avg_salary", F.col("total_salary")/F.col("total_years"))

df.show()

# 6) Add a flag (set value to True) in salaryData if the average salary of the person is lower than the average salary for their job role

salary_df = join_df.select("salary.*", "flag")

salary_df.show()
