import smtplib
from email.mime.text import MIMEText

from .. import app

mail_server = app.config['MAIL_SERVER']
mail_from = app.config['MAIL_FROM']
mail_user = app.config['MAIL_USER']
mail_password = app.config['MAIL_PASSWORD']


def send_html_mail(to_list, sub, content):
    msg = MIMEText(content, _subtype='html')
    msg['Subject'] = sub
    msg['From'] = mail_from
    msg['To'] = '; '.join(to_list)

    sender = smtplib.SMTP()
    sender.connect(mail_server)
    sender.login(mail_user, mail_password)
    sender.sendmail(mail_from, to_list, msg.as_string())
    sender.close()
