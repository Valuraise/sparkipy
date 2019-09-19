from __future__ import absolute_import
from writers.awriter import Awriter
import subprocess


class BigQueryWriter(Awriter):
    """
    /!\ you should call clean manually
    """

    def __init__(self, spark):
        self.spark = spark
        self.output_directory = None

    def write(self, df, params):
        mode = params.mode
        dataset = params.dataset
        table = params.table
        sc = self.spark.SparkContext()
        bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
        if mode == "overwrite":
            replace = 'replace'
        else:
            replace = "no-replace"
        # Shell out to bq CLI to perform BigQuery import.
        output_directory = 'gs://{' \
                           '}/hadoop/tmp/bigquery/pyspark_output/{}/{}'.format(
            bucket, dataset, table)
        df.write.format('json').save(output_directory)
        output_files = output_directory + '/part-*'
        subprocess.check_call(
            'bq load --source_format NEWLINE_DELIMITED_JSON '
            '--{replace} '
            '--autodetect '
            '{dataset}.{table} {files}'.format(replace=replace,
                                               dataset=dataset, table=table,
                                               files=output_files
                                               ).split())

    def clean(self):
        """
        You must call this method to clear temporary resources/directories
        :return: 
        """
        if self.output_directory:
            sc = self.spark.SparkContext()
            output_path = sc._jvm.org.apache.hadoop.fs.Path(
                self.output_directory)
            output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
                output_path, True)
