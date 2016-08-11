class AppError(Exception):
    """docstring for StatsError
          this class defines Errors on stats
    """

    # General
    MISSING_PARAM = (100000,
                     'can not find param "{param}" in app config')
    FORMAT_ERROR = (100001,
                    'format error found in {param}')
    TYPE_ERROR = (100002,
                  'type error found, "{param}" should be "{expect_type}"')
    DB_TAG_ERROR = (100003,
                    'data source error, {tag}')
    INVALID_DATA = (100004,
                    'invalid data found')

    CLASS_PARAM_MISSING = (110000,
                           'need param "{param}" to init class, but not found')
    NO_ATTR = (110001,
               'attribute {attr} not found in object {obj}')
    NO_OBJ = (110002,
              '{obj} not found')
    FILE_NOT_FOUND = (120001,
                      'file {file} not found')
    WRONG_CODE = (120002,
                  'this is not a valid code: {code}')
    NO_DATA = (120003,
               'no data found')
    NO_DIR = (120004,
              'no dir found: {dir}')

    SQL_ERROR = (150001,
                 'sql execute exception: {sql}')

    # User
    NO_USER = (200000,
               'user not found')
    REGISTER_FAILED_USER_EXISTS = (200001,
                                   'email {email} had been registered')
    WRONG_PASSWORD = (200002,
                      'incorrect password, please try again')

    # Query
    UNKNOWN_SOURCE = (300001,
                      'unknown data source: {source}')
    INVALID_FILTER_FORMAT = (300002,
                             'invalid filter format found')

    # Line
    NO_X_AXIS = (400001,
                 'x_axis is not defined correctly')

    # Funnel
    NO_BASE_ID = (500001,
                  'no base ids found when dealing id {itemid}')

    # DATABASE
    INVALID_QUERY = (900001,
                     'not a valid query: {query}')

    def __init__(self, name, **kargs):
        if name not in dir(self):
            raise RuntimeError('Undefined Error: {0}'.format(name))

        self.error = getattr(self, name)
        self.code = self.error[0]
        if kargs:
            self.message = str.format(self.error[1], **kargs)
        else:
            self.message = self.error[1]

    def __str__(self):
        return repr(self.message)
