from flask import g

from .error import AppError


class BaseModel(object):
    """Base Class of all models

    """

    AppError = AppError
    table = ''
    columns = {
        'id': {'default': 0, 'type': int},
    }

    def __init__(self, **params):
        super().__init__(**params)
        self.mysql_db = getattr(g, 'mysql', None)

    @classmethod
    def check_params(cls, params):
        '''Check if params is correct type

        Check params one by one if their type is correct,
        Throw a TYPE_ERROR AppError if faild

        arguments:
        params -- list of params needs to be checked
        params should be passed as [[param1, int], [param2, str]]
        '''
        if not isinstance(params, list):
            raise AppError('TYPE_ERROR', param=params, expect_type='list')

        for param_pair in params:
            if len(param_pair) == 2:
                if not isinstance(param_pair[0], param_pair[1]):
                    raise AppError('TYPE_ERROR',
                                   param=params,
                                   expect_type='list')
            else:
                raise AppError('FORMAT_ERROR', param=param_pair)

    def load(self, **params):
        '''Load obj
           Load obj data using values defined in self.columns

        arguments:
        **params, these params will be transfered to key=value
        in select where clauses

        throw AppError NO_USER if no user found
        '''
        cols = list(self.columns.keys())
        select_cols = ', '.join(cols)
        where_clauses = []
        for k, v in params.items():
            if k not in self.columns.keys():
                raise AppError('NO_ATTR',
                               attr='k',
                               obj=self.__class__.__name__)

            if self.columns[k]['type'] in (int,):
                where_clauses.append('{}={}'.format(k, v))
            else:
                where_clauses.append("{}='{}'".format(k, v))

        sql = 'SELECT {cols} FROM {table} WHERE {where}'.format(
            cols=select_cols,
            table=self.table,
            where=' and '.join(where_clauses))

        # load obj
        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            obj = cursor.fetchone()

        if not obj:
            raise self.AppError('NO_OBJ', obj=self.__class__.__name__)
        for i in range(len(cols)):
            setattr(self, cols[i], obj[i])

    def get_data(self, table, cols, clauses):
        '''get data from database
           return data from one data table on specific condition

        arguments:
        cols -- list of columns should be selected
        clauses -- list of condition clauses, such as ["id>1", "name='larry'"]
        '''
        select_cols = ', '.join(cols)
        where_clauses = ' and '.join(clauses)

        sql = 'SELECT {cols} FROM {table} WHERE {where}'.format(
            cols=select_cols,
            table=table,
            where=where_clauses)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            obj = cursor.fetchall()

        return obj
