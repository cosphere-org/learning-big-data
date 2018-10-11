
import json

# from exercise import Top10UsersJob
from answer import Top10UsersJob


def test_job():
    job = Top10UsersJob(args=['./resources/0.xml'])

    results = []
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append(value)
    
    assert len(results) == 1
    assert [user['reputation'] for user in results[0]] == [
        '9877', 
        '9760', 
        '9638', 
        '9382', 
        '9342', 
        '9069', 
        '8863', 
        '8823', 
        '8809', 
        '8791',
    ]