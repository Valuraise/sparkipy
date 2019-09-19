from sparkipy.readers.areader import Areader


class CloudStorageReader(Areader):
    """
    Read data from google cloud storage
    """

    def __init__(self, spark):
        self.spark = spark

    def read(self, path, format, delimiter=','):
        if format == "csv":
            return self.read_csv(path, delimiter)
        elif format == 'parquet':
            return self.read_parquet(path)
        elif format == "orc":
            return self.read_orc(path)
        elif format == "text":
            return self.read_text(path, delimiter)
        else:
            raise Exception('Format {} is not supported yet'.format(format))

    def read_csv(self, path, delimiter):
        return self.spark.read.option('header', 'true').option('delimiter',
                                                               delimiter).csv(
            path)

    def read_text(self, path, delimiter):
        raise Exception('Not yet implemented')

    def read_parquet(self, path, delimiter):
        raise Exception('Not yet implemented')

    def read_orc(self, path, delimiter):
        raise Exception('Not yet implemented')
