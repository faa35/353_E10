import sys
import re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


pagecounts_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('bytes', types.LongType()),
])


def filename_to_hour(path):


    #'.../pagecounts-20160801-120000.gz' is nedded as '20160801-12'

    filename = path.split('/')[-1]
    match = re.search(r'pagecounts-(\d{8})-(\d{2})', filename)
    if match is None:
        return None
    return match.group(1) + '-' + match.group(2)


path_to_hour = functions.udf(filename_to_hour, returnType=types.StringType())


def main(in_directory, out_directory):
    pagecounts = spark.read.csv(in_directory, sep=' ', schema=pagecounts_schema).withColumn(
        'filename', functions.input_file_name())
    pagecounts = pagecounts.withColumn('hour', path_to_hour(pagecounts['filename']))

    pagecounts = pagecounts.filter(
        (pagecounts['language'] == 'en')
        & (pagecounts['title'] != 'Main_Page')
        & (~ pagecounts['title'].startswith('Special:')))

    #to find the per hour max
    #then in the join, so caching it
    pagecounts = pagecounts.select('hour', 'title', 'views').cache()

    max_views = pagecounts.groupBy('hour').agg(
        functions.max('views').alias('max_views'))



    most_viewed = pagecounts.join(max_views, on='hour').filter(
        functions.col('views') == functions.col('max_views'))
    
    most_viewed = most_viewed.select('hour', 'title', 'views').sort('hour', 'title')


    #averages_by_score.write.csv(out_directory + '-score', mode='overwrite')
    most_viewed.write.csv(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
