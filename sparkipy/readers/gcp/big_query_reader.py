from __future__ import absolute_import
from readers.areader import Areader


class BigQueryReader(Areader):
    """
    /!\ you should call clean manually when you tear down the job
    """

    def __init__(self, spark):
        Areader.__init__(spark)
        self.input_directory = None

    def read(self, parmas):
        dataset = parmas.dataset
        table = parmas.table
        sc = self.spark.SparkContext()

        # Use the Cloud Storage bucket for temporary BigQuery export data used
        # by the InputFormat. This assumes the Cloud Storage connector for
        # Hadoop is configured.
        bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
        project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
        self.input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input/{' \
                               '}/{}'.format(
            bucket, dataset, table)

        conf = {
            # Input Parameters.
            'mapred.bq.project.id': project,
            'mapred.bq.gcs.bucket': bucket,
            'mapred.bq.temp.gcs.path': self.input_directory,
            'mapred.bq.input.project.id': project,
            'mapred.bq.input.dataset.id': dataset,
            'mapred.bq.input.table.id': table,
        }

        # Load data in from BigQuery.
        table_data = sc.newAPIHadoopRDD(
            'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'com.google.gson.JsonObject',
            conf=conf)
        # TODO
        # transform to data frame , what about schema !!!!
        return table_data

    def clean(self):
        if self.input_directory:
            sc = self.spark.SparkContext()
            input_path = sc._jvm.org.apache.hadoop.fs.Path(
                self.input_directory)
            input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
                input_path, True)
