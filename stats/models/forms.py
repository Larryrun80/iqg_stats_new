from flask_wtf import Form  # , RecaptchaField
from wtforms import (BooleanField,
                     TextField,
                     PasswordField,
                     DecimalField,
                     DateField,
                     validators, )

from .error import AppError


class BaseFilterForm(Form):
    # def __init__(self, *args, **kwargs):
    #     kwargs['csrf_enabled'] = False
    #     super(BaseFilterForm, self).__init__(*args, **kwargs)

    @classmethod
    def load_filters(cls, filters):
        '''Load Filter Fields
            Init a filter form using filter settings

        arguments:
            filters: a dict list with information to form a filter
            with format for each item:
                0. id: unique english name for field
                1. name: always same as the colums name to be filtered
                2. type: using following types
                            - str:  this type will only be used for search
                            - float:  this type will only be filtered
                                      with a min and max val
                            - date:  this type will only be filtered with
                                     an earliest date and a latest date

        throw a INVALID_FILTER_FORMAT error when filters not obey this rule
        '''
        must_keys = ('id', 'name', 'type')

        # check if filters' format is correct
        for item in filters:
            for mk in must_keys:
                if mk not in item.keys():
                    raise AppError('INVALID_FILTER_FORMAT')

        # init the filter
        for item in filters:
            if item['type'] == 'str':
                setattr(cls, item['id'], TextField(item['name']))
            if item['type'] == 'float':
                setattr(cls,
                        '{}_min'.format(item['id']),
                        DecimalField('min value of {}'.format(item['name']),
                                     [validators.optional()]))
                setattr(cls,
                        '{}_max'.format(item['id']),
                        DecimalField('max value of {}'.format(item['name']),
                                     [validators.optional()]))
            if item['type'] == 'date':
                setattr(cls,
                        '{}_earlier'.format(item['id']),
                        DateField('earlier {}'.format(item['name']),
                                  [validators.optional()]))
                setattr(cls,
                        '{}_later'.format(item['id']),
                        DateField('later {}'.format(item['name']),
                                  [validators.optional()]))


class RegistrationForm(Form):
    username = TextField('Username', [validators.Length(min=4, max=25)])
    realname = TextField('Realname', [validators.Length(min=2, max=5)])
    email = TextField('Email Address', [validators.Length(min=6, max=35),
                                        validators.DataRequired(),
                                        validators.Email()])
    mobile = TextField('Mobile', [validators.Length(11),
                                  validators.DataRequired()])
    password = PasswordField('New Password', [
        validators.Required(),
        validators.EqualTo('confirm', message='Passwords must match')
    ])
    # recaptcha = RecaptchaField()
    confirm = PasswordField('Repeat Password')
    accept_tos = BooleanField('I accept the TOS', [validators.Required()])


class EmailPasswordForm(Form):
    email = TextField('Email Address', [validators.Length(min=6, max=35),
                                        validators.DataRequired(),
                                        validators.Email()])
    password = PasswordField('New Password', [validators.Required()])
    # recaptcha = RecaptchaField()


class EmailForm(Form):
    email = TextField('Email Address', [validators.Length(min=6, max=35),
                                        validators.DataRequired(),
                                        validators.Email()])


class PasswordForm(Form):
    password = PasswordField('New Password', [validators.Required()])
