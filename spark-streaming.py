#importing libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions

#initialize spark session
spark=SparkSession \
        .builder \
        .appName("RetailDataAnalysis") \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

#defining schema to read invoice data
retailSchema = StructType().add("items", ArrayType(StructType() \
                    .add("SKU", StringType()) \
                    .add("title", StringType()) \
                    .add("unit_price", DoubleType()) \
                    .add("quantity", IntegerType())))\
                    .add("invoice_no", LongType()) \
                    .add("country",StringType())\
                    .add("timestamp",TimestampType()) \
                    .add("type",StringType())

#loading streaming data from kafka server into a dataframe
lines=spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","18.211.252.152:9092") \
        .option("subscribe","real-time-project") \
        .option("startingOffset","latest") \
        .load()

#casting dataframe to the defined schema
retail_df = lines\
    .selectExpr("cast (value as string) as json")\
    .select(functions.from_json(functions.col("json"), retailSchema).alias("retailData"))

#udf to calculate item count for each invoice
def get_item_count(items):
    c=0
    for i in items:
        c=c+(i.quantity)
    return c

#udf to calculate total cost for each invoice
def get_total_cost(items):
    c=0
    for i in items:
        c=c+(i.quantity*i.unit_price)
    return c
   
itemCount = udf(get_item_count, IntegerType())
totalCost = udf(get_total_cost, DoubleType())

# adding derived columns to dataframe 
retail_df=retail_df.withColumn("total_cost", totalCost("retailData.items")) 
retail_df=retail_df.withColumn("total_items", when(col("retailData.type") == "ORDER",itemCount("retailData.items")).otherwise(-(itemCount("retailData.items")))) 
retail_df=retail_df.withColumn("is_order", when(col("retailData.type") == "ORDER",1).otherwise(0))
retail_df=retail_df.withColumn("is_return", when(col("retailData.type") == "ORDER",0).otherwise(1))

#creating new dataframe as per schema of a single order
retail_agg_df=retail_df[["retailData.invoice_no","retailData.country","retailData.timestamp","total_cost","total_items","is_order","is_return"]]

#Code to write the final summarised input values to the console
consoleOutput= retail_agg_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

#calculating time based kpis for a tumbling window of 1 minute and watermark of 1 minute to handle late data
time_based_kpi=retail_agg_df\
                .withWatermark("timestamp","1 minute")\
                .groupBy(window("timestamp","1 minute","1 minute"))\
                .agg(sum("total_cost").alias("total_sales_volume"),\
                     count("invoice_no").alias("OPM"),\
                     avg("is_return").alias("rate_Of_return"),\
                     avg("total_cost").alias("average_transaction_size"))\
            .select("window","OPM","total_sales_volume","rate_of_return","average_transaction_size")

#calculate time & country based kpis for a tumbling window of 1 minute and watermark of 1 minute to handle late data
time_country_based_kpi=retail_agg_df\
                .withWatermark("timestamp","1 minute")\
                .groupBy(window("timestamp","1 minute","1 minute"),"country")\
                .agg(sum("total_cost").alias("total_sales_volume"),\
                     count("invoice_no").alias("OPM"),\
                     avg("is_return").alias("rate_Of_return"))\
            .select("window","OPM","total_sales_volume","rate_of_return")

#writing time based KPI to json file                             
timeBasedOutput= time_based_kpi \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("truncate", "false") \
        .option("path","time_kpi") \
        .option("checkpointLocation","time_checkpoint") \
        .trigger(processingTime="1 minute") \
        .start()
                     
#writing time country based KPI to json file                              
timeCountryBasedOutput= time_country_based_kpi \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("truncate", "false") \
        .option("path","time_country_kpi") \
        .option("checkpointLocation","time_country_checkpoint") \
        .trigger(processingTime="1 minute") \
        .start()

#awaiting termination from source
timeCountryBasedOutput.awaitTermination()
