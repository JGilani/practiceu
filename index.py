#I have added commnet in py file

from pyspark import SparkConf, SparkContext
import collections

def parseline(line):
  fields = line.split(',')
  stationID = fields[0]
  entryType = fields[2]
  temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
  return (stationID, entryType, temperature)

sc = SparkContext.getOrCreate()
lines = sc.textFile('/FileStore/tables/Temperature.txt')
parsedLines = lines.map(parseline)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
stationTemps.collect()
#minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# minTemps.collect()

#Now we are checking Pull
