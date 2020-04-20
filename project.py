from pyspark.sql.types import IntegerType
import time

#create dataframe from csv file.
df = sqlContext.read.csv("us-counties.csv")
#see how first three rows look like
df.show(n=3)
#to see column names
df.schema.names 
#cast a column of cases into integer
df = df.withColumn('_c4', df['_c4'].cast(IntegerType()))
#create list 
casedf = df.select("_c2", "_c4").collect()

#timing ...
start_time = time.time()
#parallelize the list to 4 cores
casedf = sc.parallelize(df.select("_c2", "_c4").collect(),4)
#reduceByKey will show each states' cumulative case count out of 50k reports
mappedf = casedf.reduceByKey(lambda accum, n:accum+n)
print(mappedf.collect())
#ending time.
print("--- %s seconds ---" % (time.time() - start_time))
sys.exit('flushing buffer and exitting the program.')



"""RDD version of the csv.
RDD does not have indexing, and contains information as chrs.
loading dataset as list of tuples.
    #take 2nd column (state)
    #convert to int on 4th column (cases reported)"""
covidRDD = sc.textFile("us-counties.csv") \
    .map(lambda line: line.split(",")) \
    .filter(lambda line: len(line)>1) \
    .map(lambda line: (line[2],int(line[4]))) \
    .collect()
# timing ...
start_time = time.time()   
#parallelize the RDD list to 4 cores.
covidlist = sc.parallelize(covidRDD,4)

#reducing states and cases by key
reducedCOVID = covidlist.reduceByKey(lambda accum, n:accum+n)
print(reducedCOVID.collect())
#ending time
print("--- %s seconds ---" % (time.time() - start_time))
sys.exit('flushing buffer and exitting the program.')