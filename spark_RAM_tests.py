from operator import add
import os, sys, numpy

"""
    A simple script exploring examples of different spark partitioners on structure of resulting partitions.

    Uses os.system(...) to sequentially execute spark standalone with 2 executors, while varying:
    
        - executor-memory (500MB to 5GB)
        - executor-memoryOverhead (500MB to 5GB)
        - size of dense numpy multiarrays in each initial RDD element (64bit; 65M entries each, 1 to 10 copies)

    Expected behavior is Out-of-Memory (OOM) errors for large numbers/sizes of numpy multiarrays. Note that

        65M 64-bit entries = ~0.5 GB

    so e.g. OOM is expected when 

        executor-memory + executor-memoryOverhead = 1GB
    
            &
        
        RDD element = {5 x 65M 64-bit multiarrays}

    since this requires ~2.5 GB per RDD element (excluding any space reduction from serializaton).

    Key questions are:
    
        - when single RDD elements *can* fit in memory, will we see OOM errors still? How often?
        - is there some >1 # of RDD elements per partition that seem to be held in RAM simultaneously?
        - when reduceByKey is used, does RAM footprint only double, or is it RAM-load-inefficient?
"""

def sparkOomCheck(sparkCoresMax, sparkExecutorCores, multiArrayEntries, numLargeObjects, executorMainRAM, executorOverheadRAM, rddLength, numParts):
    """
        Primary function. Executes simple spark jobs w/ requested settings using spark-submit through os.system; checks for OOM error.
    """

    cmd = "spark-submit.cmd --master local[2]"
    cmd += f" --driver-memory {2*executorMainRAM}M --executor-memory {executorMainRAM}M"
    cmd += f" --conf spark.driver.maxResultSize=0G "    #--conf spark.executor.memoryOverhead {executorOverheadRAM}G"
    cmd += f" --conf spark.cores.max={sparkCoresMax} --conf spark.executor.cores={sparkExecutorCores}"
    cmd += " spark_RAM_test_mapper.py"
    cmd += f" {multiArrayEntries} {numLargeObjects} {rddLength} {numParts}"

    print(f"\n\n---------------------------\n\nTrying cmd: {cmd}\n-------------------\n")
    try:
        os.system(cmd)
    except Exception as e:
        print("Exception encountered while running spark job:")
        print("sys is currently handling exception:")
        print(sys.exc_info()[0])
        print("Exception caught by Python:")
        print(e)

def main():
    #rddLength, sparkCoresMax, sparkExecutorCores, multiArrayEntries = 1000, 2, 1, 65000000
    rddLength, sparkCoresMax, sparkExecutorCores, multiArrayEntries = 1000, 2, 1, 6500000
    for numParts in (2, 10, 100):
        for numLargeObjects in (5, 15, 25): # Number of numpy multiarrays per initial RDD element
            for executorMainRAM in (500, 1000): # MB of on-heap RAM per executor
                #for executorOverheadRAM in (0.5, 1, 5): # GB of off-heap RAM per executor
                for executorOverheadRAM in (1,):
                    sparkOomCheck(sparkCoresMax, sparkExecutorCores, multiArrayEntries, numLargeObjects, executorMainRAM, executorOverheadRAM, rddLength, numParts)

if __name__ == "__main__":
    main()
