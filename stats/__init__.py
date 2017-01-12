import os

from flask import Flask, g, render_template

from .models.jsonencoder import FlaskJSONEncoder


app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config.default')
app.config.from_pyfile('config.py')
try:
    app.config.from_envvar('APP_CONFIG_FILE')
except:
    pass
app.basedir = os.path.abspath(os.path.dirname(__file__))
app.json_encoder = FlaskJSONEncoder

# markdown
from flaskext.markdown import Markdown
Markdown(app)

# mail
from flask_mail import Mail
mail = Mail(app)

# profile
# from flask.ext.profile import Profiler
# Profiler(app)

# Toolbar
# from flask_debugtoolbar import DebugToolbarExtension
# toolbar = DebugToolbarExtension(app)

# bcrypt
from flask_bcrypt import Bcrypt

bcrypt = Bcrypt(app)

# exception and database
from .models.mysql import init_mysql


@app.before_request
def init_request():
    # mysql settings
    g.mysql = init_mysql()


@app.teardown_request
def teardown(exception):
    db = getattr(g, 'mysql', None)
    if db is not None:
        db.close()

# Flask-login
from flask_login import LoginManager
from .models.user import User, Anonymous
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'user.signin'
login_manager.anonymous_user = Anonymous


@login_manager.user_loader
def load_user(userid):
    try:
        return User(id=userid)
    except:
        return User()

# flask-principal
from flask_principal import Principal, identity_loaded, RoleNeed
principals = Principal(app)


@identity_loaded.connect
def on_identity_loaded(sender, identity):
    try:
        user = User(id=identity.id)

        for role in user.roles:
            identity.provides.add(RoleNeed(role))
    except:
        pass

# blueprint
from .views.home import home
from .views.user import bp_user
from .views.stats import bp_stats
from .views.assistance import bp_ass
from .views.kits import kits
from .views.custom_stats import cs

app.register_blueprint(home)
app.register_blueprint(bp_user, url_prefix='/account')
app.register_blueprint(bp_stats, url_prefix='/data')
app.register_blueprint(bp_ass, url_prefix='/assistance')
app.register_blueprint(kits, url_prefix='/kits')
app.register_blueprint(cs, url_prefix='/cs')

# error page
@app.errorhandler(404)
def file_not_found(error):
    return render_template('error/404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error/500.html'), 500

# error log
if not app.debug:
    import logging
    from logging.handlers import SMTPHandler
    credentials = None
    if app.config['MAIL_USERNAME'] and app.config['MAIL_PASSWORD']:
        credentials = (app.config['MAIL_USERNAME'],
                       app.config['MAIL_PASSWORD'])
    mail_handler = SMTPHandler((app.config['MAIL_SERVER'],
                                app.config['MAIL_PORT']),
                               app.config['MAIL_DEFAULT_SENDER'],
                               app.config['ADMIN_MAIL'],
                               'iqg stats failure',
                               credentials)
    mail_handler.setLevel(logging.ERROR)
    app.logger.addHandler(mail_handler)

    from logging.handlers import TimedRotatingFileHandler
    log_file = '{dir}/{file}'.format(dir=app.basedir,
                                     file=app.config['ERROR_LOG'])
    if not os.path.exists(os.path.split(log_file)[0]):
        os.makedirs(os.path.split(log_file)[0])
    file_handler = TimedRotatingFileHandler(log_file,
                                            when='midnight',
                                            interval=1,
                                            backupCount=0)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    app.logger.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info('iqg stats startup')
