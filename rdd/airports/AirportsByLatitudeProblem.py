from pyspark import SparkContext, SparkConf
import re


class Utils:
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


if __name__ == "__main__":
    conf = SparkConf().setAppName("AirportsInUSA").setMaster("local[*]")
    # local[*] takes all available cores

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    airports = sc.textFile("../../in/airports.text")

    lat4o = airports.filter(lambda x: float(Utils.COMMA_DELIMITER.split(x)[6]) >= 40.0)
    lat4o.saveAsTextFile("../../in/lat40airpors.txt")

    '''
    Create a Spark program to read the airport data from in/airports.text,  
    find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    ...
    '''
