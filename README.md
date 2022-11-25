# glue-job-testing-with-custom-lib
Testing glue jobs with custom lib and glue-libs docker

This repo shows a simple way to mock for custom libs when testing for AWS glue job scripts.

The stuff presented here may be useful when writing GlueJobs using existing team-shared libs in S3. It also demonstrates a basic testing structure for multiple jobs simultaneously (which may be needed when deploying a large collection of jobs).

__NOTE:__ You may want to use more advanced mocking tools such as moto to enhance this experience even further.

```sh
> pytest -v
================================================================================================= test session starts ==================================================================================================
platform linux -- Python 3.7.10, pytest-6.2.3, py-1.11.0, pluggy-0.13.1 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /home/glue_user/workspace/jupyter_workspace
plugins: anyio-3.6.1
collected 2 items                                                                                                                                                                                                      

test/test_jobs.py::test_job1 PASSED                                                                                                                                                                              [ 50%]
test/test_jobs.py::test_job2 PASSED                                                                                                                                                                              [100%]

=================================================================================================== warnings summary ===================================================================================================
test/test_jobs.py::test_job1
test/test_jobs.py::test_job2
  /home/glue_user/spark/python/pyspark/sql/context.py:79: DeprecationWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.
    DeprecationWarning)

-- Docs: https://docs.pytest.org/en/stable/warnings.html
============================================================================================ 2 passed, 2 warnings in 12.14s ============================================================================================
```