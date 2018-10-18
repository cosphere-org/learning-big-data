
# from exercise import ReduceSideWithBloomFilterJob
from answer import ReduceSideWithBloomFilterJob


def test_job():
    job = ReduceSideWithBloomFilterJob(args=[
        './resources/0-users.xml',
        './resources/0-badges.xml',        
    ])

    results = []
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append(value)
    
    assert len(results) == 233
    assert results[0]['user']['id'] == '2975952'
    assert len(results[0]['badges']) == 1    