import argparse
import importlib
import sys

from pyspark.sql import SparkSession


class Runner:
    def __init__(self, job_name, job_args):
        self.job_name = job_name
        self.job_args = job_args
        self.spark = SparkSession \
            .builder \
            .appName('Sparkipy_Job_{}'.format(self.job_name)) \
            .getOrCreate()

    def start(self):
        # start spark session

        # create a job instance dynamically
        print(
            "Running job {} with args {}".format(self.job_name, self.job_args))
        job_class = getattr(
            importlib.import_module("jobs.{}".format(self.job_name)),
            self.job_name)
        print("Job {} found,starting ... ".format(job_name))
        job = job_class(self.spark, self.job_args)
        # run the job
        job.run()
        # Clean all temporary resources (readers for instance create temporary
        # buckets)
        job.clean()
        # stop spark session
        self.spark.stop()


def parse_args(args):
    """
    Initialize command line parser using argparse.

    Parameters
    ----------
    args : Sequence[str]
        Raw command line parameters

    Return
    ------
    parse_args : argparse.Namespace
        The arguments parsed
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--job',
        help='job name',
        required=True
    )
    parser.add_argument(
        '--args',
        help='Job args dictionary',
        required=True
    )

    return parser.parse_args(args=args)


if __name__ == '__main__':
    args = parse_args(args=sys.argv[1:])
    job_name = args.job
    job_args = {}
    if args.args is not None:
        for kv in args.args.split(','):
            key, value = kv.split("=")
            job_args[key] = value
    Runner(job_name, job_args).start()
