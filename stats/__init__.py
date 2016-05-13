from flask import Flask, g

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config.default')
app.config.from_pyfile('config.py')
app.config.from_envvar('APP_CONFIG_FILE')

# markdown
from flaskext.markdown import Markdown
Markdown(app)

# Toolbar
from flask_debugtoolbar import DebugToolbarExtension
toolbar = DebugToolbarExtension(app)

# bcrypt
from flask_bcrypt import Bcrypt

bcrypt = Bcrypt(app)

# Flask-login
from flask_login import LoginManager
from .models.user import User
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'user.signin'


@login_manager.user_loader
def load_user(userid):
    return User(id=userid)

# exception and database
from .models.mysql import init_db


@app.before_request
def init_request():
    # mysql settings
    g.mysql = init_db()
    print('mysql1: {}'.format(g.mysql))


@app.teardown_request
def teardown(exception):
    print('closing database {}'.format(g.mysql))
    db = getattr(g, 'mysql', None)
    if db is not None:
        db.close()

# blueprint
from .views.home import home
from .views.user import bp_user

app.register_blueprint(home)
app.register_blueprint(bp_user, url_prefix='/account')
