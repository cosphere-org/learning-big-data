
import json

# from exercise import RandomSamplingJob
from answer import RandomSamplingJob


def test_job():
    job = RandomSamplingJob(args=['./resources/0.xml'])

    results = set()
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.add((key, value))

    assert abs(len(results) / 10 ** 3 - 1) < 0.05         