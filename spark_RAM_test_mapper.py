import pyspark, sys
import numpy

def getMultiarrays(numLargeObjects, multiArrayEntries):
    """
        Used to generate simple numpy arrays of desired size.
    """
    multiarrays = [numpy.random.random(multiArrayEntries) for i in range(numLargeObjects)]
    return multiarrays

def addMultiArrays(A, B, numLargeObjects):
    return [numpy.add(A[i], B[i]) for i in range(numLargeObjects)]

def main():
    print(f"\n\n\n***------------------***\nRunning RAM test mapper with primary args:")
    argNames = ["multiArrayEntries","numLargeObjects","rddLength","numParts","2*executorMainRAM"]
    for i in range(argNames):
        print(f"{argNames[i]} -> sys.argv[i+1]")

    multiArrayEntries, numLargeObjects = int(sys.argv[1]), int(sys.argv[2])
    rddLength, numParts = int(sys.argv[3]), int(sys.argv[4])
    
    sc = pyspark.SparkContext()
    #sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")

    rdd = sc.parallelize([i for i in range(rddLength)], numParts).map(lambda datum: (datum%2, getMultiarrays(numLargeObjects, multiArrayEntries)))
    print(f"After initial multiarray creation, rdd has {rdd.getNumPartitions()} partitions & {rdd.count()} elements")

    print("Trying to reduceByKey & collect...")
    reducedRdd = rdd.reduceByKey(lambda A, B: addMultiArrays(A, B, numLargeObjects)).collect()
    print("reducedRdd:")
    print(reducedRdd)

if __name__ == "__main__":
    main()
