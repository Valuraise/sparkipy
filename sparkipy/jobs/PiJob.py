from operator import add
from random import random

from sparkipy.jobs.ajob import AJob
from sparkipy.utils.utils import SparkLogger


class PiJob(AJob):
    """
    this is a hello world job to compute pi value 
    """

    def __init__(self, spark, args):
        AJob.__init__(self, spark, args)

    def run(self):
        partitions = self.args['partitions']
        n = 100000 * int(partitions)

        def f(_):
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1 if x ** 2 + y ** 2 <= 1 else 0

        count = self.spark.sparkContext.parallelize(range(1, n + 1),
                                                    partitions).map(f).reduce(
            add)
        print("Pi is roughly %f" % (4.0 * count / n))
        SparkLogger.info("Pi is roughly %f" % (4.0 * count / n))
