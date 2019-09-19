from setuptools import setup, find_packages

setup(name='sparkipy',
      python_requires='>=3.6',
      version='0.0.4',
      author="Wajdi FATHALLAH",
      author_email="wfathallah@valuraise.com",
      url="https://github.com/Valuraise/sparkipy",
      description='Framework for pyspark jobs',
      test_suite='tests',
      classifiers=[
          "Programming Language :: Python :: 3",
          "Operating System :: OS Independent",
      ],
      packages=find_packages(),
      zip_safe=False)
