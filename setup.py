import  setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='sparkipy',
      python_requires='>=3.6',
      version='0.0.11',
      author="Wajdi FATHALLAH",
      author_email="wfathallah@valuraise.com",
      url="https://github.com/Valuraise/sparkipy",
      long_description=long_description,
      long_description_content_type="text/markdown",
      description='Framework for pyspark jobs',
      test_suite='tests',
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent",
      ],
      packages=setuptools.find_packages(),
      zip_safe=False)
