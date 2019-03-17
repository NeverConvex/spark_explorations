import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring
from operator import add
import matplotlib.pyplot as plt

"""
    A simple script exploring examples of different spark partitioners on structure of resulting partitions.

    To execute, on a Windows machine with standalone spark:

        spark-submit.cmd --master local[2] spark_test.py

    (if in a cluster w/ yarn or mesos, change master and other cmd-line params as desired)
"""

smallCsvFile = "example.csv"
largeCsvFile = "bigExample.csv"
SMALL_NUM_REPARTITIONS = 4      # Choose how many partitions to use in each repartitioning strategy for small examples.
LARGE_NUM_REPARTITIONS = 100    # Choose how many partitions to use in each repartitioning strategy for large examples.
#LARGE_NUM_RECORDS = 1000        # Choose how many records from the large, fake CA file to use (max: approx 55 million).
AGG_LEN = 4

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def smallRddExample(sc):
    assert True == False, "Thou shallt not pass. ('cuz this an't implemented yet)"

def largeRddExample(sc):
    assert True == False, "Thou shallt not pass. ('cuz this an't implemented yet)"

#def makeBarChart():

def countDistinctGeocodes(partition, geocodeLen=6):
    distinctGeocodes = set([])
    for record in partition:
        geocode = record[0][:geocodeLen]
        distinctGeocodes.add(geocode)
    return len(distinctGeocodes)

def listOfGeocodes(partition, geocodeLen=6):
    distinctGeocodes = set([])
    for record in partition:
        geocode = record[0][:geocodeLen]
        distinctGeocodes.add(geocode)
    return distinctGeocodes

def getAndPrintRddStatistics(df, mode="small"):
    rdd = df.rdd.glom() # Convert rdd to partitions equivalent

    # If small example, explicitly display rdd partitions in STDOUT
    if mode == "small":
        print(f"The df.rdd contains {rdd.getNumPartitions()} partitions:")
        for partition in rdd.collect():
            print(partition)

    # How many partitions were there in total?
    numPartitions = rdd.getNumPartitions()
    print(f"Found {numPartitions} partitions in rdd")

    # Of those, how many partitions were empty?
    numEmpty = rdd.map(lambda partition: 1 if len(partition) == 0 else 0).reduce(add)
    print(f"Of those, {numEmpty} were empty")

    # What was the distribution of partition sizes?
    sizes = rdd.map(lambda partition: (len(partition), 1) ).reduceByKey(add).collect()
    print("distribution of partition sizes:")
    print(sizes)

    lengths = [6,7,11] if mode == "large" else [1,2,3]
    # For each geolevel, how many distinct tract, county, block codes were there in total?
    tracts = rdd.map(lambda partition: listOfGeocodes(partition, lengths[0])).reduce(set.union)#.map(lambda setOfGeocodes: len(setOfGeocodes)).collect()
    print(f"number of tracts in total: {len(tracts)}")
    blockgroups = rdd.map(lambda partition: listOfGeocodes(partition, lengths[1])).reduce(set.union)#.map(lambda setOfGeocodes: len(setOfGeocodes)).collect()
    print(f"number of blockgroups in total: {len(blockgroups)}")
    blocks = rdd.map(lambda partition: listOfGeocodes(partition, lengths[2])).reduce(set.union)#.map(lambda setOfGeocodes: len(setOfGeocodes)).collect()
    print(f"number of blocks in total: {len(blocks)}")

    # For each geolevel, how many distinct tract, county, or block codes were in each partition?
    numTracts = rdd.map(lambda partition: (countDistinctGeocodes(partition, geocodeLen=lengths[0]), 1)).reduceByKey(add).collect()
    totalTracts = sum([a*b for a, b in numTracts])
    print(f"distribution of #tracts per partition, & total (note redundancy):")
    print(numTracts)
    print(f"total: {totalTracts}")
    numBlockgroups = rdd.map(lambda partition: (countDistinctGeocodes(partition, geocodeLen=lengths[1]), 1)).reduceByKey(add).collect()
    totalBlockgroups = sum([a*b for a, b in numBlockgroups])
    print(f"distribution of #blockgroups per partition:")
    print(numBlockgroups)
    print(f"total: {totalBlockgroups}")
    numBlocks = rdd.map(lambda partition: (countDistinctGeocodes(partition, geocodeLen=lengths[2]), 1)).reduceByKey(add).collect()
    totalBlocks = sum([a*b for a, b in numBlocks])
    print(f"distribution of #blocks per partition:")
    print(numBlocks)
    print(f"total: {totalBlocks}")

    # On avg, for each geolevel, how many partitions was each tract, county, or block code in?
    # (not yet implemented; a little more complicated than distribution of #geounits per partition)

    print("---------------------\n")

def dfAnalysisDriver(sc, mode="small"):
    """
        This example is too big for prints of glom().collect() to work, so we instead measure:

            - On avg for each geolevel, how many partitions was each tract, county, or block code in?
            - How many partitions were empty?
            - How many partitions were there in total?
            - What was the distribution of partition sizes?
    """
    print(f"*** Executing dataframes analysis driver in mode {mode} ***")
    numRepartitions = LARGE_NUM_REPARTITIONS if mode=="large" else SMALL_NUM_REPARTITIONS
    csvFile = largeCsvFile if mode=="large" else smallCsvFile

    sqlContext = SQLContext(sc)
    print(f"Loading {csvFile} into dataframe...")
    df1 = sqlContext.read.format("csv").option("header", "true").load(csvFile)
    df1.printSchema()

    df1 = df1.withColumn("partitionCode", substring("geocode",0,AGG_LEN))                   # Initial df
    df2 = df1.repartition(numRepartitions)                                                  # Default hash code repartitioning
    df3 = df1.repartition(numRepartitions, 'partitionCode').drop('partitionCode')           # hashCode repartition based on partitionCode
    df4 = df1.repartitionByRange(numRepartitions, 'partitionCode').drop('partitionCode')    # range repartition based on partitionCode
    dfs = [df1, df2, df3, df4]
    dfLabels = ["initial","hash default repartition","hash repartition by partitionCode", "range repartition by partitionCode"]
    for df, dfLabel in zip(dfs, dfLabels):
        print(f"Processing example spark dataframe: {dfLabel}")
        print("dataframe schema:")
        df.printSchema()
        getAndPrintRddStatistics(df, mode=mode)

    print("<--- Done analysis. ---> \n\n")

def main():
    sc = pyspark.SparkContext()
    quiet_logs(sc)
    dfAnalysisDriver(sc, mode="small")
    dfAnalysisDriver(sc, mode="large")

if __name__ == "__main__":
    main()
