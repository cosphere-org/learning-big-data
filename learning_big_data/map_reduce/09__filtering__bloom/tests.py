
# from exercise import NotHotFilterJob
from answer import NotHotFilterJob


def test_job():
    job = NotHotFilterJob(args=['./resources/1.xml'])

    results = []
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append(value)
    
    assert len(results) == 94265