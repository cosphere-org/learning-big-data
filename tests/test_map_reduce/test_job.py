
from unittest import TestCase
import textwrap
import xmltodict

import httpretty
import pytest

from map_reduce.job import Job


class MockParallel:

    def __init__(self, n_jobs):
        pass

    def __call__(self, calls):
        return list(calls)


def mock_delayed(fn):

    def inner(*args, **kwargs):
        return fn(*args, **kwargs)

    return inner


class JobTestCase(TestCase):

    @pytest.fixture(autouse=True)
    def initfixtures(self, mocker, tmpdir):
        self.tmpdir = tmpdir
        self.mocker = mocker

        self.mocker.patch('map_reduce.job.Parallel', MockParallel)
        self.mocker.patch('map_reduce.job.delayed', mock_delayed)

    @httpretty.activate
    def test_run_simple_job__map_only(self):

        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/0.xml',
            textwrap.dedent('''
                <row id="1" name="Hero"></row>
                <row id="2" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))
        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/1.xml',
            textwrap.dedent('''
                <row id="2" name="Winner"></row>
                <row id="2" name="Star"></row>
                <row id="3" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))

        outputs_dir = self.tmpdir.mkdir('outputs')

        self.mocker.patch.object(
            Job, 'get_output_path').return_value = str(outputs_dir)

        def record_reader(line):
            record = dict(
                xmltodict.parse(line.decode('utf-8'))['row'])

            yield (
                record['@id'],
                {
                    k.replace('@', '').lower(): v
                    for k, v in record.items()
                })

        def mapper(key, value):
            yield (value['name'], 1)

        Job(
            input_uris=[
                'http://s3.data/0.xml',
                'http://s3.data/1.xml',
            ],
            record_reader=record_reader,
            mapper=mapper,
            config={
                'num_of_mappers': 1,
            }).run()

        assert len(outputs_dir.listdir('*.json')) == 2
        assert (
            outputs_dir.join('mapper_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Hero", "values": [1]}
                {"key": "Star", "values": [1]}
                {"key": "Maniac", "values": [1]}
            ''').strip())
        assert (
            outputs_dir.join('mapper_1.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Winner", "values": [1]}
                {"key": "Star", "values": [1, 1]}
                {"key": "Maniac", "values": [1]}
            ''').strip())

    @httpretty.activate
    def test_run_simple_job__no_combiner(self):

        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/0.xml',
            textwrap.dedent('''
                <row id="1" name="Hero"></row>
                <row id="2" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))
        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/1.xml',
            textwrap.dedent('''
                <row id="2" name="Winner"></row>
                <row id="2" name="Star"></row>
                <row id="3" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))

        outputs_dir = self.tmpdir.mkdir('outputs')

        self.mocker.patch.object(
            Job, 'get_output_path').return_value = str(outputs_dir)

        def record_reader(line):
            record = dict(
                xmltodict.parse(line.decode('utf-8'))['row'])

            yield (
                record['@id'],
                {
                    k.replace('@', '').lower(): v
                    for k, v in record.items()
                })

        def mapper(key, value):
            yield (value['name'], 1)

        def reducer(key, values):
            yield (key, sum(values))

        Job(
            input_uris=[
                'http://s3.data/0.xml',
                'http://s3.data/1.xml',
            ],
            record_reader=record_reader,
            mapper=mapper,
            reducer=reducer,
            config={
                'num_of_mappers': 1,
                'num_of_reducers': 1,
            }).run()

        assert len(outputs_dir.listdir('*.json')) == 3
        assert (
            outputs_dir.join('mapper_0__partition_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Hero", "values": [1]}
                {"key": "Star", "values": [1]}
                {"key": "Maniac", "values": [1]}
            ''').strip())
        assert (
            outputs_dir.join('mapper_1__partition_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Winner", "values": [1]}
                {"key": "Star", "values": [1, 1]}
                {"key": "Maniac", "values": [1]}
            ''').strip())
        assert (
            outputs_dir.join('reducer_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Hero", "value": 1}
                {"key": "Star", "value": 3}
                {"key": "Maniac", "value": 2}
                {"key": "Winner", "value": 1}
            ''').strip())

    @httpretty.activate
    def test_run_simple_job(self):

        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/0.xml',
            textwrap.dedent('''
                <row id="1" name="Hero"></row>
                <row id="2" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))
        httpretty.register_uri(
            httpretty.GET,
            'http://s3.data/1.xml',
            textwrap.dedent('''
                <row id="2" name="Winner"></row>
                <row id="2" name="Star"></row>
                <row id="3" name="Star"></row>
                <row id="1" name="Maniac"></row>
            '''))

        outputs_dir = self.tmpdir.mkdir('outputs')

        self.mocker.patch.object(
            Job, 'get_output_path').return_value = str(outputs_dir)

        def record_reader(line):
            record = dict(
                xmltodict.parse(line.decode('utf-8'))['row'])

            yield (
                record['@id'],
                {
                    k.replace('@', '').lower(): v
                    for k, v in record.items()
                })

        def mapper(key, value):
            yield (value['name'], 1)

        def reducer(key, values):
            yield (key, sum(values))

        Job(
            input_uris=[
                'http://s3.data/0.xml',
                'http://s3.data/1.xml',
            ],
            record_reader=record_reader,
            mapper=mapper,
            combiner=reducer,
            reducer=reducer,
            config={
                'num_of_mappers': 1,
                'num_of_reducers': 1,
            }).run()

        assert len(outputs_dir.listdir('*.json')) == 3
        assert (
            outputs_dir.join('mapper_0__partition_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Hero", "values": [1]}
                {"key": "Star", "values": [1]}
                {"key": "Maniac", "values": [1]}
            ''').strip())
        assert (
            outputs_dir.join('mapper_1__partition_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Winner", "values": [1]}
                {"key": "Star", "values": [2]}
                {"key": "Maniac", "values": [1]}
            ''').strip())
        assert (
            outputs_dir.join('reducer_0.json').read().strip() ==
            textwrap.dedent('''
                {"key": "Hero", "value": 1}
                {"key": "Star", "value": 3}
                {"key": "Maniac", "value": 2}
                {"key": "Winner", "value": 1}
            ''').strip())
