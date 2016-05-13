from flask_wtf import Form  # , RecaptchaField
from wtforms import BooleanField, TextField, PasswordField, validators


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
