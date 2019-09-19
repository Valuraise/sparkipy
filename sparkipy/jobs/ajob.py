from pyspark.sql.dataframe import DataFrame


class AJob:
    def __init__(self, spark, args):
        self.spark = spark
        self.args = args

        # Monkey patch the DataFrame object with a transform method so we
        # can chain DataFrame transformations.

        def transform(self, f):
            return f(self)

        DataFrame.transform = transform

    def run(self):
        pass

    def clean(self):
        """
        This is for cleaning operations before tearing down the project, 
        rewrite this in your job if needed
        :return: 
        """
        pass
