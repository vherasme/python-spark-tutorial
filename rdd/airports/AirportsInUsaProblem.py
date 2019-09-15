from pyspark import SparkContext, SparkConf
import re


class Utils:
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


def split_comma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":
    conf = SparkConf().setAppName("AirportsInUSA").setMaster("local[4]")
    # local[*] takes all available cores

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    airports = sc.textFile("../../in/airports.text")

    usa_airports = airports.filter(lambda x: x.split(",")[3] == "\"United States\"")

    citiesAndAirports = usa_airports.map(split_comma)
    citiesAndAirports.saveAsTextFile("../../in/airportsfiltered.text")

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''
