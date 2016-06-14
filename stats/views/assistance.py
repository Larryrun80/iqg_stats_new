from flask import (Blueprint,
                   render_template,
                   request)

from ..models.dailyitem import DailyItemCollector

bp_ass = Blueprint('assistance', __name__)


@bp_ass.route('/dailyitems', methods=['GET'])
def dailyitems():
    date = request.args.get('date', None)
    di = DailyItemCollector(date)
    data = di.get_diff()
    return render_template('assistance/dailyitems.html', data=data)
