from flask.ext.login import AnonymousUserMixin

from .base import BaseModel
from .. import bcrypt


class Anonymous(AnonymousUserMixin):
    def __init__(self):
        self.id = 0
        self.username = 'Guest'
        self.realname = 'Guest'
        self.password = ''
        self.email = '-'
        self.actived = False
        self.enabled = False


class User(BaseModel):
    """docstring for User"""

    table = 'users'
    columns = {
        'id': {'default': None, 'type': int},
        'username': {'default': None, 'type': str},
        'realname': {'default': None, 'type': str},
        'password': {'default': None, 'type': str},
        'email': {'default': None, 'type': str},
        'actived': {'default': False, 'type': int},
        'enabled': {'default': False, 'type': int},
    }

    def __init__(self, **params):
        '''Init user
           Load user data and return, using id or email/password

        arguments:
        1. load user via id:
            use User(id=userid), userid should be int
        2. load user via email/password:
            use User(email=email, password=pwd)

        throw AppError NO_USER if no user found
        '''
        super().__init__()

        for key, val in self.columns.items():
            setattr(self, key, val['default'])

        if params:
            if {'id'} == set(params.keys()) \
                    and params['id']:
                self.load(**params)
            if {'email'} == set(params.keys()):
                self.load(**params)
            if {'password', 'email'} == set(params.keys()):
                # get user info
                password = self.get_user_by_email(
                    params['email'])['password']

                if self.check_password(params['password'], password):
                    self.load(email=params['email'])
                else:
                    raise self.AppError('WRONG_PASSWORD')
        self.update_status()

    def get_user_by_email(self, email):
        cols = ['email', 'password']
        clauses = ["email='{}'".format(email)]
        user = self.get_data(self.table, cols, clauses)
        if not user:
            raise self.AppError('NO_USER')

        return_data = {}
        for i in range(len(cols)):
            return_data[cols[i]] = user[0][i]

        return return_data

    def generate_password(self, plaintext):
        '''Return a bcrypt hashed password

        '''
        return bcrypt.generate_password_hash(plaintext).decode()

    def set_password(self, plaintext):
        sql = '''
                UPDATE users
                   SET password='{password}',
                       updated_at=now()
                 WHERE id={id}
        '''.format(password=self.generate_password(plaintext),
                   id=self.id)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            self.mysql_db.commit()

    def check_password(self, plaintext, password):
        '''Return if password is match

        '''
        return bcrypt.check_password_hash(password, plaintext)

    def create_user(self, username, realname, email, mobile, plain_password):
        '''Create new user

        arguments:
        username -- username of user, string
        realname -- realname of user, string
        email -- email of user, string, email format
        plain_password -- plain password of user, string
        '''

        self.check_params([[username, str], [realname, str], [email, str],
                          [mobile, str], [plain_password, str]])
        password = self.generate_password(plain_password)

        if self.get_data(self.table, ['id'], ["email='{}'".format(email)]):
            raise self.AppError('register_failed_user_exists', email=email)

        sql = '''
                INSERT INTO users
                    (username, realname, email, password, mobile,
                    actived, enabled, created_at, updated_at)
                    VALUES
                    ('{username}', '{realname}', '{email}',
                     '{password}', '{mobile}', 0, 1, now(), now())
                ;
              '''.format(username=username,
                         realname=realname,
                         email=email,
                         password=password,
                         mobile=mobile)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            self.mysql_db.commit()

    def confirm_user(self, email):
        '''Confirm user
        active user when they active from email

        arguments:
        email -- email of user, string, email format

        returns:
        return the count of confirm users
        '''
        sql = '''
                UPDATE users
                   SET actived=1,
                       actived_at=now(),
                       updated_at=now()
                 WHERE email='{}'
                   AND actived=0
              '''.format(email)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            self.mysql_db.commit()
            updated_row = cursor.rowcount

        return updated_row

    def update_status(self):
        self.is_authenticated = bool(self.enabled and self.actived)
        self.is_active = bool(self.enabled)
        self.is_anonymous = bool(self.username is None)
        self.roles = self.get_roles(self.id)

    def get_id(self):
        return self.id

    def get_roles(self, uid=None):
        if uid is None:
            if self.id is None:
                return ['anonymous']
            elif not self.actived:
                return ['inactive']

        sql = '''
                SELECT r.role
                  FROM users u
             LEFT JOIN user_role ur ON u.id=ur.user_id
             LEFT JOIN roles r on r.id=ur.role_id
                 WHERE u.id={uid}
        '''.format(uid=self.id)

        roles = []
        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()

        if result == ((None,), ):  # if no role designed, give Anonymous
            roles = ['Anonymous']
        else:
            for (role,) in result:  # if roles designed, update user's roles
                if role:
                    roles.append(role)

        return roles

    def is_favourite(self, route):
        sql = '''
                SELECT count(0)
                FROM   user_favourite
                WHERE  user_id={uid}
                AND    route='{route}'
              '''.format(uid=self.id, route=route)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

        return result[0] > 0

    def add_favourite(self, route):
        if not route or not isinstance(route, str):
            raise self.AppError('INVALID_DATA')

        sql = '''
                INSERT INTO user_favourite (user_id, route)
                SELECT * FROM (SELECT {uid}, '{route}') AS tmp
                WHERE NOT EXISTS
                (SELECT id
                 FROM   user_favourite
                 WHERE  user_id={uid}
                 AND    route='{route}')
                LIMIT 1
              '''.format(uid=self.id, route=route)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            self.mysql_db.commit()

        return True

    def remove_favourite(self, route):
        if not route or not isinstance(route, str):
            raise self.AppError('INVALID_DATA')

        sql = '''
                DELETE FROM user_favourite
                WHERE       user_id={uid}
                AND         route='{route}'
              '''.format(uid=self.id, route=route)

        with self.mysql_db.cursor() as cursor:
            cursor.execute(sql)
            self.mysql_db.commit()

        return True
