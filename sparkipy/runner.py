import argparse
import importlib
import sys

from pyspark.sql import SparkSession


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


def job_factory(spark, name, args):
    """
    Create dynamically a job object
    :param spark: spark session
    :param name: job name
    :param args: job args
    :return: an instance of job to be run
    """
    print("Running job {} with args {}".format(name, args))
    job_class = getattr(
        importlib.import_module("jobs.{}".format(name)),
        name)
    return job_class(spark, args)


def start(job_name, job_args):
    # start spark session
    spark = SparkSession \
        .builder \
        .appName('Sparkipy_Job_{}'.format(job_name)) \
        .getOrCreate()
    # create a job instance dynamically
    job = job_factory(spark, job_name, job_args)
    print("Job {} found,starting ... ".format(job_name))
    # run the job
    job.run()
    # Clean all temporary resources (readers for instance create temporary
    # buckets)
    job.clean()
    # stop spark session
    spark.stop()


if __name__ == '__main__':
    args = parse_args(args=sys.argv[1:])
    job_name = args.job
    job_args = {}
    if args.args is not None:
        for kv in args.args.split(','):
            key, value = kv.split("=")
            job_args[key] = value
    start(job_name, job_args)
