import decimal
from past.builtins import unicode
from .gcloud import GCLOUD as gcloud
from .job_queue import JobQueue

from google.cloud import bigquery


class BigQuery():
    me = None

    @staticmethod
    def querybuilder(**kwargs):
        return QueryBuilder(**kwargs)

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = BigQuery(env)
        return cls.me

    def __init__(self, env):
        project = gcloud.project(env)
        self.client = bigquery.Client(project=project)

    def create_queue(self, func=None):
        if not func:
            func = self._query
        return JobQueue(self.me, func)

    def _query(self, sql, args=[]):
        if isinstance(sql, QueryBuilder):
            (query, args_) = sql()
        else:
            query = sql
            args_ = args
        config = bigquery.QueryJobConfig(query_parameters=args_)
        return self.client.query(query, job_config=config)

    def get_job(self, *args, **kwargs):
        return self.client.get_job(*args, **kwargs)

    def query(self, sql, args=[]):
        job = self._query(sql, args)
        while job.state != 'DONE':
            job = self.client.get_job(job.job_id, location=job.location)
        return list(job.result())


class QueryBuilder():
    """
    Based on previous work in this php library:
    https://github.com/energylab/gacela/blob/master/library/Gacela/DataSource/Query/Sql.php

    @author N Goodrich
    @date 4/5/2020
    """

    def __call__(self):
        return (str(self), self._params)

    def __str__(self):
        def _should(key):
            return key in self._builder

        if _should('insert'):
            return self._build_insert()

        if _should('union'):
            return self._build_union()

        if _should('select'):
            select = self._build_select()

            if _should('from'):
                select += self._build_from()

            if _should('join'):
                select += self._build_select('join')

            if _should('where'):
                select += self._build_where()

            if _should('order'):
                select += self._build_order()

            if 'limit' in self._builder:
                select += self._build_limit()

            return select

    def __init__(self, **kwargs):
        self._builder = {}
        self._params = []

        def _build(k, v):
            if k[-1] == '_':
                k = k[:-1]
            method = getattr(self, f'_{k}', None)
            if callable(method):
                method(v)
            else:
                self._builder[k] = v

        for k, v in kwargs.items():
            _build(k, v)

    def _add_param(self, param):
        def _type(value, type_):
            if type_ is not None:
                pass
            if isinstance(value, (str, unicode)):
                type_ = 'STRING'
            elif isinstance(value, int):
                type_ = 'INT64'
            elif isinstance(value, float):
                type_ = 'FLOAT'
            elif isinstance(value, decimal.Decimal):
                type_ = 'NUMERIC'
            elif value.__class__.__name__ in ['datetime', 'date']:
                pass
            elif isinstance(value, (list, tuple)):
                pass
            elif isinstance(value, dict):
                pass
            else:
                raise Exception(f'Unknown value type: {value.__class__.__name__} passed for {key}')
            return type_

        def _param(key, type_, value):
            if type_ not in ['ARRAY', 'STRUCT']:
                param = bigquery.ScalarQueryParameter(key, type_, value)
            elif type_ == 'ARRAY':
                type_ = {map(_type, value)}
                if len(type_) > 1:
                    raise Exception(f'array has too many types: {type_}')
                param = bigquery.ArrayQueryParameter(key, type_[0], value)
            elif type_ == 'STRUCT':
                args = [key]
                for k, v in value.items():
                    t_ = _type(v)
                    args.append(_param(k, t_, v))
                param = bigquery.StructQueryParameter(*args)
            return param

        key = param[0]
        value = param[1]
        self._params.append(
            _param(key,
                   _type(value, (lambda a: None if len(a) < 3 else a[2])(param)), value))

    def _add_params(self, params):
        for p in params:
            self._add_param(p)

    def _build_insert(self):
        _insert = self._builder['insert']
        fields = _insert[0].keys()
        _sql = f"INSERT INTO {self._builder['table']} ({', '.join(fields)}) VALUES\n"

        for row in _insert:
            _sql += '('+','.join(map(
                lambda x: f"'{x}'" if isinstance(x, str) else 'NULL' if x is None else str(x), row.values()))+'),\n'

        return _sql[:-2]

    def _build_select(self, type_='select'):
        sql = ''
        for field in self._builder[type_]:
            if sql != '':
                sql += ',\n'
            alias = None
            if isinstance(field, tuple):
                alias = field[1]
                field = field[0]

            sql += f'{field}'

            if alias is not None:
                sql += f' AS {alias}'

        sql += '\n'
        if type_ == 'select':
            return f'SELECT\n{sql}'
        elif type_ == 'join':
            return sql

    def _build_from(self):
        def _f(f):
            tbl = ''
            if isinstance(f[0], (str, unicode)):
                tbl = f[0]
            if isinstance(f[0], self.__class__):
                tbl, params = f[0]()
                tbl = f'({tbl})'
                self._params.extend(params)

            if f[1] != '':
                tbl += f' AS {f[1]}'
            return tbl

        fr = self._builder['from']
        if not isinstance(fr, list):
            fr = [fr]
        sql = ''
        for f in fr:
            if not isinstance(f, tuple):
                f = (f, '')
            if sql != '':
                sql += ',\n'
            sql += _f(f)
        return f'FROM {sql}\n'

    def _build_join(self):
        for join in self._builder['join']:
            pass

    def _build_where(self):
        sql = ''
        for where in self._builder['where']:
            conj = 'AND'
            qry = None
            params = []
            if isinstance(where, (str, unicode)):
                qry = where
            elif isinstance(where, self.__class__):
                qry, params = where()
            elif isinstance(where, tuple):
                if isinstance(where[0], self.__class__):
                    qry, params = where[0]()

                    if len(where) > 1:
                        conj = where[1].upper()
                else:
                    qry = where[0]
                    params = [where[1]]

                    if len(where) > 2:
                        conj = where[2].upper()

            if sql != '':
                sql += f'{conj} {qry}\n'
            else:
                sql = f'{qry}\n'

            self._add_params(params)

        return f'WHERE\n{sql}'

    def _group(self):
        pass

    def _having(self):
        pass

    def _build_order(self):
        sql = ''
        for a in self._builder['order']:
            if sql != '':
                sql += ',\n'
            if isinstance(a, (str, unicode)):
                a = (a, 'asc')
            sql += f'{a[0]} {a[1].upper()}'
        return f'ORDER BY\n{sql}\n'

    def _build_limit(self):
        limit = self._builder['limit']
        if isinstance(limit, tuple):
            pass
        else:
            return f'LIMIT {limit}'

    def _build_union(self):
        sql = ''
        type_ = self._builder['union'][0]
        unions = self._builder['union'][1]
        for union in unions:
            if sql != '':
                sql += f'UNION {type_.upper()}\n'
            if isinstance(union, self.__class__):
                query, args = union()
            elif isinstance(union, tuple):
                query = union[0]
                args = union[1]
            else:
                query = union
                args = []

            sql += query+'\n'
            self._params.extend(args)
        return sql

    def _update(self):
        pass

    def _build_with(self):
        sql = ''
        for tbl, qry in self._builder['with']:
            if sql != '':
                sql += ',\n'
            if isinstance(qry, self.__class__):
                query, args = qry()
            elif isinstance(qry, tuple):
                query = qry[0]
                args = qry[1]
            else:
                query = qry
                args = []

            sql += query
            self._params.extend(args)
        return sql+'\n'

    def _build_delete(self):
        pass
