
import os
from uuid import uuid4

import requests


class Context:

    def __init__(self):
        self.events = []

    def write(self, key, value):
        self.events.append([key, value])

    def collect_to_dict(self):
        collected = {}
        for key, value in self.events:
            collected.setdefault(key, [])
            collected[key].append(value)

        return collected


def default_output_format(events):
    try:
        os.makedirs('./.outputs/')

    except FileExistsError:
        pass

    output_path = f'./.outputs/{uuid4()}.txt'
    with open(output_path, 'w') as f:
        output = '\n'.join([
            '{0},{1}'.format(*event)
            for event in events
        ])
        f.write(output)

    return output_path


class Job:

    def __init__(
            self,
            input_uris,
            record_reader,
            mapper,
            reducer,
            output_format=None):

        self.input_uris = input_uris
        self.record_reader = record_reader
        self.mapper = mapper
        self.reducer = reducer
        self.output_format = output_format or default_output_format

    def run(self):

        # -- record_reader
        mapper_context = Context()
        for input_uri in self.input_uris:
            response = requests.get(input_uri, stream=True)

            records = self.record_reader(response)

            # -- map tasks
            for record in records:
                self.mapper(
                    key=record[0], value=record[1], context=mapper_context)

        # -- reduce task
        reducer_context = Context()
        for key, values in mapper_context.collect_to_dict().items():
            self.reducer(key, values, reducer_context)

        # -- output format
        return self.output_format(reducer_context.events)
