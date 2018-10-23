
import json

# from exercise import BadgeMinMaxCountPerUserJob
from answer import BadgeMinMaxCountPerUserJob


def test_job():
    job = BadgeMinMaxCountPerUserJob(args=['./resources/0.xml'])

    results = []    
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.append((key, value))

    assert len(results) == 80987
    
    # -- top 3 and bottom 3 should be ok
    results = sorted(results, key=lambda x: x[0])
    
    assert results[:3] + results[-3:] == [
        ('1', ['2017-12-04T04:46:55.950', '2017-12-04T04:46:55.950', 1]), 
        ('1000090', ['2017-11-30T10:18:46.063', '2017-11-30T10:18:46.063', 1]), 
        ('1000115', ['2017-12-03T03:19:13.543', '2017-12-03T03:19:13.543', 1]), 
        ('99989', ['2017-12-01T13:27:50.407', '2017-11-29T11:36:38.533', 4]), 
        ('999896', ['2017-11-29T09:02:10.143', '2017-11-29T09:02:10.143', 1]), 
        ('999942', ['2017-12-01T14:45:44.430', '2017-11-30T15:39:54.080', 2]),
    ]