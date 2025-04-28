import apache_beam as beam
import logging


class Log(beam.DoFn):
    def __init__(self, func='info'):
        self.__func__ = getattr(logging.getLogger(), func)

    def process(self, payload):
        self.__func__(f'>> logging payload: {payload}')
        yield payload
