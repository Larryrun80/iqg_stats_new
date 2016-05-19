from flask import url_for, render_template

from .. import app
from .error import AppError
from ..utils.security import ts
from ..utils.mail import send_html_mail


def send_active_email(user, email_type):
    valid_email_conf = {
        'confirm': {
            'subject': 'iqg stats 注册确认',
            'salt': app.config['CONFIRM_SALT'],
            'url': 'user.confirm_email',
            'template': 'email/activate.html'
        },
        'resend': {
            'subject': 'iqg stats 注册确认',
            'salt': app.config['CONFIRM_SALT'],
            'url': 'user.confirm_email',
            'template': 'email/activate.html'
        },
        'recover': {
            'subject': 'iqg stats 密码重置',
            'salt': app.config['RECOVER_SALT'],
            'url': 'user.confirm_email',
            'template': 'email/recover.html'
        },
    }

    if email_type not in valid_email_conf.keys():
        raise AppError('TYPE_ERROR', param=email_type,
                       expect_type='valid active mail type')
    conf = valid_email_conf[email_type]

    # send registion mail
    subject = conf['subject']
    token = ts.dumps(user.email, salt=conf['salt'])
    url = url_for(
        conf['url'],
        token=token,
        _external=True
    )
    html = render_template(conf['template'],
                           url=url,
                           service_mail=app.config['SERVICE_MAIL'])

    send_html_mail([user.email], subject, html)
