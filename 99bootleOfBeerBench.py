# Databricks notebook source
from pyspark.sql import types as T
from pyspark.sql import functions as F



# COMMAND ----------

def plur(intxt):
  return "" if intxt == "1" else "s"
def countToStr(count): 
  return str(count) if count>0 else "no more"

def line(count):
  """Lyrics line for :count: beers"""
  return """%(cou)s bottle%(plu)s of beer on the wall, %(cou)s bottle%(plu)s of beer.\nTake one down and pass it around, %(coum)s bottle%(plum)s of beer on the wall.\n\n"""%{
    "cou":countToStr(count),
    "coum":countToStr(count-1)  ,
    "plu":plur(countToStr(count)),
    "plum":plur(countToStr(count-1))  
  }
def lineList(counts):
  """Lyrics for serveral Lines"""
  return counts.map(line)

# COMMAND ----------



# COMMAND ----------

def lyrSizeSpark(COUNT): 
  """counting character count for :COUNT: inital bottle of beer
  Full pyspark version
  """
  df = spark.createDataFrame( sc.range(COUNT,0,-1) ,schema=T.IntegerType())

  df = df.withColumn("count", F.when(F.col("value") >0, F.col("value").astype(T.StringType()) ).otherwise("no more")) 
  df = df.withColumn("countm", F.when(F.col("value")-1 >0, (F.col("value")-1).astype(T.StringType()) ).otherwise("no more")) 
  df = df.withColumn("plu", F.when(F.col("count") == "1", "" ).otherwise("s")) 
  df = df.withColumn("plum", F.when(F.col("countm") == "1", "" ).otherwise("s")) 

  df = df.withColumn("lyr",F.format_string("""%s bottle%s of beer on the wall, %s bottle%s of beer.\nTake one down and pass it around, %s bottle%s of beer on the wall.\n\n""",
                              F.col("count"),F.col("plu"),F.col("count"),F.col("plu"),F.col("countm"),F.col("plum"))   )
  
  return df.withColumn("lyrc", F.length(F.col("lyr")) ).select(F.sum(F.col("lyrc")).alias("c")).first()["c"]

def lyrSizeMixed(count): 
  """
  using udf 
  """
  df = spark.createDataFrame( sc.range(count,0,-1) ,schema=T.IntegerType())
  df = df.withColumn("lyr",F.udf(line)(F.col("value"))).select("lyr")
  return df.withColumn("lyrc", F.length(F.col("lyr")) ).select(F.sum(F.col("lyrc")).alias("c")).first()["c"]

def lyrSizePandasMixed(count): 
  """
  using pandas udf 
  """
  df = spark.createDataFrame( sc.range(count,0,-1) ,schema=T.IntegerType())
  df = df.withColumn("lyr",F.pandas_udf(lineList,T.StringType())(F.col("value"))).select("lyr")
  return df.withColumn("lyrc", F.length(F.col("lyr")) ).select(F.sum(F.col("lyrc")).alias("c")).first()["c"]

def lyrSizePy(count):
  """python way"""
  return sum((len(line(i)) for i in range(count,0,-1)))

# COMMAND ----------

help(F.pandas_udf)

# COMMAND ----------

def timeItFrame(t,impl,COUNT,wcount):
  df = spark.createDataFrame(sc.parallelize(t.all_runs),schema=T.DoubleType()).withColumnRenamed("value","dur")
  df = df.withColumn("impl",F.lit(impl))

  df = df.withColumn("count",F.lit(COUNT) )
  df = df.withColumn("wcount",F.lit(wcount   )) 
  
  return df

# COMMAND ----------

xsm = [5,10,100,1000]
sml = [10000,30000,60000,1000*100,1000*200,1000*300]
med=[1000*500,1000*800,1000*1000,1000*1500,1000*2000,1000*3000]
big=[1000*4000,1000*6000,1000*1000*10,1000*1000*14,1000*1000*16,1000*1000*20,1000*1000*40,1000*1000*60]


wcount = 2
final = None
for count in xsm+sml+med+big:
  
  t = %timeit -n1 -r2 -o lyrSizeMixed(count)
  df = timeItFrame(t,"mixed",count,wcount)
  
  t = %timeit -n1 -r2 -o lyrSizeSpark(count)
  df = df.union(timeItFrame(t,"spark",count,wcount))
  
  t = %timeit -n1 -r2 -o lyrSizePandasMixed(count)
  df = df.union(timeItFrame(t,"pandas",count,wcount))
  
  if count < 900*1000:
    t = %timeit -n1 -r2 -o lyrSizePy(count)
    df = df.union(timeItFrame(t,"py",count,wcount))
    
  if final is None : 
    final = df
  else:
    final = final.union(df)
final.write.mode("append").parquet("/FileStore/99Btl.parquet")

# COMMAND ----------


display(final)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

