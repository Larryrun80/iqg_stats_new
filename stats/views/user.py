from flask import Blueprint, render_template, url_for, redirect, abort, flash,\
    request
from flask.ext.login import login_required, logout_user, login_user

from .. import app
from ..models.forms import RegistrationForm, EmailPasswordForm,\
    EmailForm, PasswordForm
from ..models.user import User
from ..utils.security import ts
from ..utils.mail import send_html_mail

bp_user = Blueprint('user', __name__)


@bp_user.route('/create', methods=['GET', 'POST'])
def create_account():
    form = RegistrationForm()

    try:
        if form.validate_on_submit():
            # create account
            User().create_user(form.username.data,
                               form.realname.data,
                               form.email.data,
                               form.mobile.data,
                               form.password.data)

            # send registion mail
            subject = 'iqg stats 注册确认'
            token = ts.dumps(form.email.data, salt=app.config['CONFIRM_SALT'])
            confirm_url = url_for(
                'user.confirm_email',
                token=token,
                _external=True
            )
            html = render_template('email/activate.html',
                                   confirm_url=confirm_url,
                                   service_mail=app.config['SERVICE_MAIL'])

            send_html_mail([form.email.data], subject, html)

            return render_template('user/active_info.html',
                                   email=form.email.data)

    except Exception as e:
        flash(e)

    return render_template('user/create.html', form=form)


@bp_user.route('/signin', methods=['GET', 'POST'])
def signin():
    form = EmailPasswordForm()

    if form.validate_on_submit():
        try:
            user = User(email=form.email.data, password=form.password.data)
            if user:
                login_user(user)
                next = request.args.get('next')
                return redirect(request.args.get(next, url_for('home.index')))
        except Exception as e:
            flash(e)

    return render_template('user/signin.html', form=form)


@bp_user.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('home.index'))


@bp_user.route("/reset", methods=["GET", "POST"])
def reset():
    form = EmailForm()
    if form.validate_on_submit():
        try:
            # get user
            user = User(email=form.email.data)
            if user.email:
                # send registion mail
                subject = 'iqg stats 密码重置'
                token = ts.dumps(user.email, salt=app.config['RECOVER_SALT'])
                recover_url = url_for(
                    'user.reset_with_token',
                    token=token,
                    _external=True
                )
                html = render_template('email/recover.html',
                                       recover_url=recover_url,
                                       service_mail=app.config['SERVICE_MAIL'])

                send_html_mail([user.email], subject, html)
                info = 'hi {user}, 我们已经向您的邮箱 {email} 发送了重置密码邮件，请根据邮件内容重置您的密码'\
                       ''.format(user=user.username, email=user.email)
                return render_template('home/notificate.html', info=info)

        except Exception as e:
            flash(e)
    return render_template('user/reset_password.html', form=form)


@bp_user.route('/confirm/<token>', methods=['GET'])
def confirm_email(token):
    try:
        email = ts.loads(token, salt=app.config['CONFIRM_SALT'], max_age=86400)
    except:
        abort(404)

    # confirm user
    User().confirm_user(email)

    info = 'hi {email}, 您的账号已成功激活。'.format(email)

    return render_template('home/notificate.html', info=info)


@bp_user.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_with_token(token):
    try:
        email = ts.loads(token, salt=app.config['RECOVER_SALT'], max_age=86400)
    except:
        abort(404)

    form = PasswordForm()

    if form.validate_on_submit():
        user = User(email=email)
        user.set_password(form.password.data)

        info = "您的密码已成功重置，点击<a href='{}'> 此处 </a>重新登陆".format(
            url_for('user.signin'))
        return render_template('home/notificate.html', info=info)

    return render_template('user/reset_with_token.html',
                           form=form,
                           token=token)


@bp_user.route('/profile')
def profile():
    return render_template('user/profile.html')
