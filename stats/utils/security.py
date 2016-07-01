import random
import string

from itsdangerous import URLSafeTimedSerializer

from .. import app

ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])


def generate_random_str(length):
    length = int(length)
    return ''.join(random.sample(string.ascii_letters + string.digits,
                                 length))
