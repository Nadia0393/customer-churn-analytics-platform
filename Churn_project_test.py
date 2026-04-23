# Databricks notebook source
print("Databricks is working 🚀")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

df_raw = spark.read.csv(
    "/Workspace/Users/nadhiya.ganesan93@gmail.com/WA_Fn-UseC_-Telco-Customer-Churn.csv",
    header=True,
    inferSchema=True
)

df_raw.show(5)

# COMMAND ----------

file_path = "/Workspace/Users/nadhiya.ganesan93@gmail.com/WA_Fn-UseC_-Telco-Customer-Churn.csv"

df_raw = spark.read.csv(file_path, header=True, inferSchema=True)

df_raw.show(5)
df_raw.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/Workspace/Users/nadhiya.ganesan93@gmail.com/"))

# COMMAND ----------

df_raw = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/Volumes/workspace/default/dataset/WA_Fn-UseC_-Telco-Customer-Churn.csv")

df_raw.show(5)

# COMMAND ----------

df_raw = spark.read.csv(
    "dbfs:/Volumes/workspace/default/dataset/WA_Fn-UseC_-Telco-Customer-Churn.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC Add ingestion timestamp
# MAGIC Save as Delta table
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_bronze=df_raw.withColumn("ingesttime",current_timestamp())
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_churn_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer
# MAGIC Convert raw data into clean,structured,analytics ready dataset 

# COMMAND ----------

df=spark.read.table("bronze_churn_raw")
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col("TotalCharges").isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col("TotalCharges")==" ").count()

# COMMAND ----------

from pyspark.sql.functions import expr

df_clean = df_clean.withColumn(
    "TotalCharges",
    expr("try_cast(TotalCharges as double)")
)

# COMMAND ----------

from pyspark.sql.functions import expr

df_clean = df_clean.withColumn(
    "TotalCharges",
    expr("try_cast(TotalCharges as double)")
)

# COMMAND ----------

#Clean the data
#Look for empty strings and replace with NULL values before casting to numeric

from pyspark.sql.functions import col,when
df_clean=df.withColumn(
    "TotalCharges",
    when(col("TotalCharges")==" ",None).otherwise(col("TotalCharges"))
)

#Convert to numeric
df_clean=df_clean.withColumn("TotalCharges",col("TotalCharges").cast("double"))

# COMMAND ----------

#Handle NULL values
df_clean=df_clean.fillna({"TotalCharges":0})


# COMMAND ----------

#Encode Churn column
df_clean=df_clean.withColumn("Churn_label",when(col("Churn")=="yes",1).otherwise(0))

# COMMAND ----------

from pyspark.sql.functions import when, col

df_clean = df_clean.withColumn(
    "churn_label",
    when(col("Churn") == "Yes", 1).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create meaningful features that help predict churn.
# MAGIC df_clean=df_clean.withColumn("Customer value score",
# MAGIC                             col("Total Charges")/col("Tenure")+1)

# COMMAND ----------

from pyspark.sql.functions import expr
df_clean=df_clean.withColumn("Customer_value_score",
                            expr("try_divide(TotalCharges,Tenure)"))
df_clean.select("Customer_value_score").show(10)

# COMMAND ----------

#Monthly spend category
df_clean=df_clean.withColumn("Monthly_spend_category",
                             when(col("MonthlyCharges")<35,"Low")
                             .when(col("MonthlyCharges")>=35,"Medium")
                             .when(col("MonthlyCharges")>70,"High"))

# COMMAND ----------

df_clean.select("TotalCharges","Monthly_spend_category").show(10)

# COMMAND ----------

#tenure group - new ,established,loyal 
#new customers churn more , loyal customers churn less
df_clean=df_clean.withColumn("Tenure_group",
                             when(col("Tenure")<12,"New")
                             .when(col("Tenure")<24,"Established")
                             .otherwise("Loyal"))

# COMMAND ----------

df_clean.select("tenure","Tenure_group").show(10)

# COMMAND ----------

#high risk customer 
#pays a lot ,joined recently - 
df_clean=df_clean.withColumn("High_risk_customer",
                             when((col("MonthlyCharges") > 70) & (col("Tenure") < 6),1) .
                             otherwise(0))

df_clean.select("MonthlyCharges","tenure","High_risk_customer").show(10)

# COMMAND ----------

df_clean.select(
    "customerID",
    "MonthlyCharges",
    "tenure",
    "Monthly spend category",
    "tenure_group",
    "high_risk_customer",
    "churn_label"
).show(10)

# COMMAND ----------

df_clean=df_clean.drop("Monthly spend category")

# COMMAND ----------


df_clean = df_clean.withColumnRenamed(
    "Monthly spend category",
    "monthly_spend_category"
)



# COMMAND ----------

df_clean=df_clean.drop("monthly_spend_category")

# COMMAND ----------

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_clean_churn")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold layer

# COMMAND ----------

df_silver=spark.table("silver_clean_churn")
df_silver.show(10)

# COMMAND ----------

#which customer group churn most ?
#what is churn rate per segment
#what is average revenue per segment ?


# COMMAND ----------

#Load silver data
silver_df=spark.read.format("delta").load("/Volumes/workspace/default/dataset/silver_clean_churn"
)
silver_df.show(10)

# COMMAND ----------

Build core KPI's
Total customers
Churned customers
churn rate
average monthly charges
average customer value

# COMMAND ----------

from pyspark.sql.functions import count,avg,sum,round

kpi_df = df_silver.select.groupBy("tenure_group").agg(
    count("*").alias("total_customers"),
    sum("churn_label").alias("churned_customers"),
    round(avg("MonthlyCharges"), 2).alias("avg_monthly_charges"),
    round(avg("customer_value_score"), 2).alias("avg_customer_value_score")
)

# COMMAND ----------

from pyspark.sql.functions import expr

df_clean = df_clean.withColumn(
    "TotalCharges",
    expr("try_cast(TotalCharges as double)")
)

# COMMAND ----------

#add churn rate - what percentage of customers have left out of total customers
kpi_df=kpi_df.withColumn("Churn_rate",
                         round((col("Churned_customers")/col("Total_customers"))*100,2))

kpi_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC