import apache_beam as beam
from decimal import Decimal


class StringifyDecimal(beam.DoFn):
    def process(self, element):
        def _stringify(el):
            for f, v in el.items():
                if isinstance(v, Decimal):
                    el[f] = str(v)
                if isinstance(v, list):
                    for i, e in enumerate(v):
                        el[f][i] = _stringify(e)
                print()
            return el
        yield _stringify(element)
