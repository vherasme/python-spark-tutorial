from pyspark import SparkContext, SparkConf


def split_lines():
    return lambda line: line.split(" ")


if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[4]")
    sc = SparkContext(conf=conf)

    sc.setLogLevel("ERROR")

    lines = sc.textFile("../in/word_count.text")

    words = lines.flatMap(split_lines())

    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))
