from flask import (Blueprint,
                   render_template,
                   request,
                   jsonify,
                   flash,
                   url_for)
from flask.ext.login import (login_required,
                             logout_user,
                             login_user,
                             current_user)

cs = Blueprint('cs', __name__)


@cs.route('/hsq_daily')
def hsq_daily():
    from ..models.custom.hsq_daily import HsqDaily
    hd = HsqDaily()
    data = hd.get_table_data()

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

    return render_template('stats/custom/custom_table.html', data=data)
