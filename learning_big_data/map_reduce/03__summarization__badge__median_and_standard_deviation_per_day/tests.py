
import json

# from exercise import BadgeMediaStdDevPerDayJob
from answer import BadgeMediaStdDevPerDayJob


def test_job():
    job = BadgeMediaStdDevPerDayJob(args=['./resources/0.xml'])

    results = []    
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append((key, value))

    results = sorted(results, key=lambda x: x[0])
    assert len(results) == 13
    
    # -- top 3 should be ok
    assert results[:3] == [
        ('2016-06-12', {'median': 14, 'stddev': None}), 
        ('2017-01-12', {'median': 9.0, 'stddev': 3.6186463676125595}), 
        ('2017-01-31', {'median': 6, 'stddev': None}),
    ]