import apache_beam as beam
import datetime
import dateutil.parser
from past.builtins import unicode


class InsertIngestionTimestamp(beam.DoFn):
    def __init__(self, **kwargs):
        self._timestamp_field = 'ingestion_timestamp' if 'field' not in kwargs else kwargs['field']

    def process(self, element):
        element[self._timestamp_field] = datetime.datetime.now()
        yield element


class StringToDatetime(beam.DoFn):
    def process(self, element):
        def _format_datetime(payload, schema):
            for f in schema:
                if f.name not in payload:
                    continue
                v = payload[f.name]
                if isinstance(v, str) and f.field_type == 'DATETIME':
                    fmt = '%Y-%m-%d %H:%M:%S.%f'
                    payload[f.name] = transform_date(v, fmt) if validate_date(v) else v
                elif isinstance(v, str) and f.field_type == 'DATE':
                    fmt = '%Y-%m-%d'
                    payload[f.name] = transform_date(v, fmt) if validate_date(v) else v
                elif isinstance(v, str) and f.field_type == 'TIMESTAMP':
                    fmt = '%Y-%m-%d %H:%M:%S.%f'
                    payload[f.name] = transform_timestamp(v) if validate_date(v) else v
                elif f.field_type == 'RECORD':
                    for i, e in enumerate(v):
                        payload[f.name][i] = _format_datetime(e, f.fields)
            return payload
        yield _format_datetime(element['payload'], element['schema'])


class StringifyDatetimes(beam.DoFn):
    def __init__(self, format=None):
        self._format = format

    def process(self, element):
        def _stringify(el):
            if not isinstance(el, dict):
                raise Exception('element is not a dict!')

            for f, v in el.items():
                if isinstance(v, list):
                    for i, e in enumerate(v):
                        el[f][i] = _stringify(e)
                elif isinstance(v, (str, unicode)) and '0000-00-00' in v:
                    el[f] = None
                elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                    el[f] = str(v) if self._format is None else v.strftime(self._format)
            return el

        yield _stringify(element)

# Utils


def validate_date(date_text):
    try:
        dateutil.parser.parse(date_text)
        return True
    except ValueError:
        return False


def transform_date(date_text, format): return dateutil.parser.parse(date_text).strftime(format)
def transform_timestamp(date_text): return dateutil.parser.parse(date_text).timestamp()
