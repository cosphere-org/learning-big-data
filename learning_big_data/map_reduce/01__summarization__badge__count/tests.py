
import json

# from exercise import BadgeCountJob
from answer import BadgeCountJob

# from exercise import BadgeCountPerUserJob
from answer import BadgeCountPerUserJob


def test_badge_count_job():
    job = BadgeCountJob(args=['./resources/0.xml'])

    results = set()    
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.add((key, value))

    # -- all records should be unique
    assert len(results) == len(set(results))
    
    results = sorted(results, key=lambda x: (x[1], x[0]), reverse=True)

    # -- top 5 should be ok
    assert results[:5] == [
        ('Popular Question', 14687), 
        ('Informed', 9557), 
        ('Notable Question', 7466), 
        ('Editor', 7230), 
        ('Yearling', 5903),
    ]
    # -- bottom 5 should be ok
    assert results[-5:][::-1] == [
        ('Census', 1), 
        ('activeadmin', 1), 
        ('ajax', 1), 
        ('alembic', 1), 
        ('amazon-web-services', 1)
    ]
    
    
def test_badge_per_user_count_job():
    job = BadgeCountPerUserJob(args=['./resources/0.xml'])

    results = []    
    with job.make_runner() as runner:
        runner.run()
        for user_id, name_counts in job.parse_output(runner.cat_output()):
            results.append((user_id, name_counts))

    # -- all records should be unique
    assert len(results) == 80987

    results = {user_id: name_counts for user_id, name_counts in results}
    assert results['1000090'] == [['Necromancer', 1]]
    assert set(tuple(r) for r in results['217408']) == set([
        ('Nice Answer', 11), 
        ('Enlightened', 8),         
        ('Good Answer', 6), 
        ('Announcer', 2),
        ('Great Answer', 1), 
        ('Guru', 1),        
        ('Popular Question', 1),         
        ('Revival', 1),
    ])