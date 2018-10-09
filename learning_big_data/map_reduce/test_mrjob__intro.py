
import mrjob__wordcount

def test_job():
    job = mrjob__wordcount.MRWordFrequencyCount(args=['hello_world.txt'])

    results = set()
    with job.make_runner() as runner:
        runner.run()
        for key, value in job.parse_output(runner.cat_output()):
            results.add((key, value))

    assert results == set([
        ('hello', 3),
        ('world', 2),
        ('how', 1),
        ('are', 1),
        ('you', 1),
        ('doing', 1),
        ('yes', 1),        
    ])