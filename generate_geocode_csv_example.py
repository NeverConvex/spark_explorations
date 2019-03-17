import numpy

#SOURCE = '/d/Users/NeverConvex/Downloads/cageo2010.sf1'
SOURCE = 'D:\\Users\\NeverConvex\\Downloads\\cageo2010.sf1'
TARGET = 'bigExample.csv'
NUM_GEOCODES = 100
NUM_RECORDS = 10000
MAX_RECORDS_PER_BLOCK = 20
MAX_VAL = 1000

def getGeocodes():
    geocodes = set([])
    rawInput = open(SOURCE, 'r').readlines()
    for line in rawInput:#[:25]:
        geocodeCols = line[54:65].strip()
        if len(geocodeCols) == 11:
            geocodes.add(geocodeCols)
            #print(geocodeCols)
    print(f"Found {len(geocodes)} distinct geocodes in {SOURCE}")
    return geocodes

def makeUniformFakeRecords(geocodes):
    print("Making uniformly fake records for all geocodes")
    records = [['geocode','value']]
    for geocode in geocodes:
        numRecs = numpy.random.randint(MAX_RECORDS_PER_BLOCK)
        recordVals = numpy.random.randint(MAX_VAL, size=numRecs)
        for val in recordVals:
            records.append([geocode,str(val)])
    return records

def writeRecordsToCsv(records):
    print(f"Writing fake records to {TARGET}")
    targetFile = open(TARGET, 'w')
    for record in records:
        targetFile.write(','.join(record)+'\n')
    targetFile.close()

def main():
    geocodes = getGeocodes()
    records = makeUniformFakeRecords(geocodes)
    writeRecordsToCsv(records)

if __name__ == "__main__":
    main()
