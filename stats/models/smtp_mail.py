import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


mail_from = 'guonan@doweidu.com'
mail_user = 'guonan@doweidu.com'
mail_password = 'larryg80'


def send_html_mail(to_list, sub, content):
    msgRoot = MIMEMultipart()
    msg = MIMEText(content, 'plain', 'utf-8')
    msgRoot['Subject'] = sub
    msgRoot['from'] = mail_from
    msgRoot['to'] = to_list
    msgRoot.attach(msg)

    smtp = smtplib.SMTP()
    smtp.connect('smtp.ym.163.com')
    smtp.login(mail_user, mail_password)
    smtp.sendmail(mail_from, to_list, msgRoot.as_string())
    smtp.quit()
