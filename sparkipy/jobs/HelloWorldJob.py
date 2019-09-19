from sparkipy.jobs.ajob import AJob
from sparkipy.utils.utils import SparkLogger


class HelloWorldJob(AJob):

    def __init__(self, spark, args):
        AJob.__init__(self, spark, args)

    def run(self):
        SparkLogger.info("running job hello world")
