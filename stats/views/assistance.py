from flask import (Blueprint,
                   render_template,
                   flash,
                   request)

from ..models.dailyitem import DailyItemCollector

bp_ass = Blueprint('assistance', __name__)


@bp_ass.route('/dailyitems', methods=['GET'])
def dailyitems():
    data = None
    try:
        date = request.args.get('date', None)
        di = DailyItemCollector(date)
        data = di.get_diff()
    except Exception as e:
        flash(e)

    return render_template('assistance/dailyitems.html', data=data)
