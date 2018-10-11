
import json

from answer import DistictReputationJob
# from exercise import DistictReputationJob


def test_job():
    job = DistictReputationJob(args=['./resources/0.xml'])

    results = []
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append(value)
    
    assert len(results) == 1751
    assert len(results) == len(set(results))