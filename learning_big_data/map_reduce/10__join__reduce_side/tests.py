
# from exercise import ReduceSideJoinJob
from answer import ReduceSideJoinJob


def test_job():
    job = ReduceSideJoinJob(args=[
        './resources/0-users.xml',
        './resources/0-badges.xml',        
    ])

    results = []
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append(value)
    
    assert len(results) == 966
    assert results[0]['user']['id'] == '2975318'
    assert len(results[0]['badges']) == 1    