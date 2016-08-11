from flask import (Blueprint,
                   render_template,
                   url_for,
                   redirect,
                   abort,
                   flash,
                   jsonify,
                   request)
from flask.ext.login import (login_required,
                             logout_user,
                             login_user,
                             current_user)
from flask_principal import identity_changed, Identity

from .. import app
from ..models.forms import (RegistrationForm,
                            EmailPasswordForm,
                            EmailForm,
                            PasswordForm)
from ..models.user import User
from ..utils.security import ts
from ..models.notificate import send_active_email
from ..models.roles import anonymous_permission

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
            user = User(email=form.email.data)
            send_active_email(user, 'confirm')

            from ..words.user import to_active_info
            info = to_active_info.format(user=user.username, email=user.email)
            return render_template('home/notificate.html', info=info)

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
                if not user.is_authenticated:
                    from ..words.user import need_active_info
                    flash(need_active_info.format(user=user.username))
                    return redirect(url_for('home.index'))
                else:
                    login_user(user)
                    next = request.args.get('next')

                    # principal identity
                    identity = Identity(user.id)
                    identity_changed.send(app, identity=identity)

                    return redirect(
                        request.args.get(next, url_for('home.index')))
        except IndexError as e:
            flash(e)

    return render_template('user/signin.html', form=form)


@bp_user.route("/resend")
def resend():
    user = current_user
    send_active_email(user, 'confirm')

    from ..words.user import mail_sent_info
    flash(mail_sent_info)
    return redirect(request.args.get(next, url_for('home.index')))


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
                send_active_email(user, 'recover')

                from ..words.user import recover_info
                info = recover_info.format(user=user.username,
                                           email=user.email)
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

    from ..words.user import actived_info
    info = actived_info.format(email=email)

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

        from ..words.user import recoverd_info
        info = recoverd_info.format(user=user.username,
                                    url=url_for('user.signin'))
        return render_template('home/notificate.html', info=info)

    return render_template('user/reset_with_token.html',
                           form=form,
                           token=token)


@bp_user.route('/profile')
def profile():
    return render_template('user/profile.html')


@bp_user.route('/fav', methods=['POST'])
@login_required
def fav():
    result = {'success': False, 'message': 'Unknown Operation'}
    user = current_user
    op = request.form['op']
    route = request.form['route']

    if op == 'n':
        if user.add_favourite(route):
            result['success'] = True
            result['message'] = ''
    elif op == 'y':
        if user.remove_favourite(route):
            result['success'] = True
            result['message'] = ''
    return jsonify(result)
