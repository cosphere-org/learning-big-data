
import json

# from exercise import ReputationJob
from answer import ReputationJob


def test_job():
    job = ReputationJob(args=['./resources/0.xml'])

    results = set()
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.add((key, value))

    assert len(results) == 49
    for _, user in results:
        assert int(json.loads(user)['reputation']) > 10000        