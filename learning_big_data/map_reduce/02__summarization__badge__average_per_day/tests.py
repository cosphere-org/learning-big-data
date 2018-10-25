
import json

# from exercise import BadgeAveragePerDayJob
from answer import BadgeAveragePerDayJob


def test_job():
    job = BadgeAveragePerDayJob(args=['./resources/0.xml'])

    results = []    
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append((key, value))

    results = sorted(results, key=lambda x: x[0])
    assert len(results) == 13
    
    # -- top 3 should be ok
    assert results[:3] == [
        ('2016-06-12', {'avg': 14.0, 'count': 1}), 
        ('2017-01-12', {'avg': 10.511699879227054, 'count': 13248}), 
        ('2017-01-31', {'avg': 6.0, 'count': 1}),
    ]