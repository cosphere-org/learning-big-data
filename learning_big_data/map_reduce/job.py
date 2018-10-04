
from glob import glob
from hashlib import md5
from joblib import Parallel, delayed
from texttable import Texttable
from time import time
from uuid import uuid4
import json
import logging
import multiprocessing
import os

import requests


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class Job:

    def __init__(
            self,
            input_uris,
            record_reader,
            mapper,
            combiner=None,
            reducer=None,
            config=None):

        self.id = str(uuid4())
        self.config = {
            'num_of_mappers': multiprocessing.cpu_count(),
            'num_of_reducers': multiprocessing.cpu_count(),
            **(config or {})
        }

        self.input_uris = input_uris
        self.record_reader = record_reader
        self.mapper = mapper
        self.combiner = combiner
        self.reducer = reducer

    def run(self):

        self.start_time = time()
        self.output_path = self.get_output_path()

        #
        # MAPPER
        #
        input_sizes = Parallel(n_jobs=self.config['num_of_mappers'])(
            delayed(self.get_mapper())(
                input_uri=input_uri,
                record_reader=self.record_reader,
                mapper=self.mapper,
                idx=i,
                map_only=self.reducer is None,
                combiner=self.combiner,
                partition=self.get_partitioner(self.config['num_of_mappers']),
                output_path=self.output_path)
            for i, input_uri in enumerate(self.input_uris))

        #
        # REDUCER
        #
        if self.reducer:

            def get_partition_filepaths(partition_idx):
                filepaths_pattern = os.path.join(
                    self.output_path,
                    f'mapper_*__partition_{partition_idx}.json')

                return glob(filepaths_pattern)

            Parallel(n_jobs=self.config['num_of_reducers'])(
                delayed(self.get_reducer())(
                    input_filepaths=get_partition_filepaths(i),
                    reducer=self.reducer,
                    idx=i,
                    output_path=self.output_path)
                for i in range(self.config['num_of_reducers']))

        #
        # JOB STATS
        #
        self.get_stats(input_sizes)

    #
    # MAPPER
    #
    def get_mapper(self):

        def inner(
                input_uri,
                record_reader,
                mapper,
                idx,
                map_only,
                combiner,
                partition,
                output_path):

            print(f'[MAPPER.{idx}] start')
            start = time()

            input_size = 0
            response = requests.get(input_uri, stream=True)
            records = []
            for line in response.iter_lines():
                if line:
                    input_size += len(line)
                    records.append(next(record_reader(line)))

            partitioned = {}
            for key, value in records:

                new_key, new_value = next(mapper(key=key, value=value))

                partition_idx = partition(new_key)

                partitioned.setdefault(partition_idx, {})
                partitioned[partition_idx].setdefault(new_key, [])
                partitioned[partition_idx][new_key].append(new_value)

            for partition_idx, partition_data in partitioned.items():
                if map_only:
                    partition_filepath = (
                        f'{output_path}/mapper_{idx}.json')
                    mode = 'a'

                else:
                    partition_filepath = (
                        f'{output_path}/'
                        f'mapper_{idx}__partition_{partition_idx}.json')
                    mode = 'w'

                with open(partition_filepath, mode) as f:
                    for key, values in partition_data.items():
                        if combiner:
                            new_key, new_value = next(combiner(key, values))

                            f.write(json.dumps({
                                'key': new_key,
                                'values': [new_value],
                            }) + '\n')

                        else:
                            f.write(json.dumps({
                                'key': key,
                                'values': values,
                            }) + '\n')

            duration = round(time() - start, 3)
            print(
                f'[MAPPER.{idx}] stop - DURATION: {duration}s - '
                f'INPUT SIZE: {input_size}')

            return input_size

        return inner

    #
    # PARTITIONER
    #
    def get_partitioner(self, num_of_reducers):

        def inner(key):
            str_key_hash = md5(key.encode('utf8')).hexdigest()
            num_key_hash = ''.join([
                str(ord(c) % num_of_reducers)
                for c in str_key_hash
            ])

            return int(num_key_hash) % num_of_reducers

        return inner

    #
    # REDUCER
    #
    def get_reducer(self):

        def inner(input_filepaths, reducer, idx, output_path):

            print(f'[REDUCER.{idx}] start')
            start = time()

            # -- shuffle and sort
            records = {}
            for filepath in input_filepaths:
                with open(filepath, 'r') as f:
                    for line in f:
                        record = json.loads(line)
                        records.setdefault(record['key'], [])
                        records[record['key']].extend(record['values'])

            # -- reducer & output
            reducer_filepath = (
                f'{output_path}/reducer_{idx}.json')
            with open(reducer_filepath, 'w') as f:
                for key, values in records.items():
                    new_key, new_value = next(reducer(key, values))

                    f.write(json.dumps({
                        'key': new_key,
                        'value': new_value,
                    }) + '\n')

            duration = round(time() - start, 3)
            print(f'[REDUCER.{idx}] stop - {duration}s')

        return inner

    def get_output_path(self):

        output_path = os.path.join(
            os.path.dirname(__file__),
            '.outputs',
            self.id)

        try:
            os.makedirs(output_path)

        except FileExistsError:
            pass

        return output_path

    def get_stats(self, input_sizes):
        filepaths = glob(os.path.join(self.output_path, '*.json'))
        table = Texttable()
        table.add_rows(
            [('filename', 'size (bytes)')] + [
                (
                    os.path.basename(filepath),
                    os.path.getsize(filepath),
                )
                for filepath in sorted(filepaths)
            ]),

        input_size = sum(input_sizes)

        duration = round(time() - self.start_time, 3)

        # -- max shuffle size
        mapper_filepaths = glob(
            os.path.join(self.output_path, 'mapper_*.json'))

        max_shuffle_size = sum(
            os.path.getsize(filepath) for filepath in mapper_filepaths)

        print(f'''

==========================================

JOB ID: {self.id}

INPUT SIZE: {input_size}

OUTPUT PATH: {self.output_path}

EXECUTION TIME: {duration}

MAX SHUFFLE SIZE: {max_shuffle_size}

FILES:
{table.draw()}
        ''')
