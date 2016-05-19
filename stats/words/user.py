to_active_info = \
    '''
hi {user},

您已经注册成功，但在激活邮箱之前，您还无法登陆我们的网站

我们已经发送了一封激活邮件到您的信箱 {email}, 请查收并点击其中的链接完成邮箱激活。
    '''

recover_info = \
    '''
hi {user},

我们已经向您的邮箱 {email} 发送了重置密码邮件，请根据邮件内容重置您的密码
    '''

actived_info = \
    '''
hi {email},

您的账号已成功激活。
    '''

recoverd_info = \
    '''
hi {user},

您的密码已成功重置，点击<a href='{url}'> 此处 </a>重新登陆
    '''

need_active_info = 'hi {user}, 您必须先激活账号才能继续访问网站'

mail_sent_info = '激活邮件已发送，请查收'
