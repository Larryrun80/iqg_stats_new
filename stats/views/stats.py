from flask import (Blueprint,
                   render_template,
                   abort,
                   flash,
                   request)
from flask.ext.paginate import Pagination

bp_stats = Blueprint('stats', __name__)


@bp_stats.route('/query/<tag>')
def query(tag):
    from ..models.query import QueryItem
    q = QueryItem(tag)
    p = None
    if not q.title:
        abort(404)

    data = {
        'title': q.title,
        'route': q.route,
        'author': q.author['author'],
        'email': q.author['email'],
    }

    current_page = request.args.get('page', 1)
    try:
        current_page = int(current_page)
    except:
        flash('page is not int')
        return render_template('stats/query.html')
    page_size = 20
    total = q.get_result_count()

    data['rows'] = q.get_result(page_size=page_size,
                                current_page=current_page)
    data['columns'] = q.columns

    # pagination
    p = Pagination(page=current_page,
                   total=total,
                   per_page=page_size,
                   record_name='users',
                   bs_version=3)

    return render_template('stats/query.html', data=data, pagination=p)


@bp_stats.route('/line/<tag>', methods=['GET', 'POST'])
def line(tag):
    from ..models.charts.lines import LineItem
    l = LineItem(tag)
    if not l.title:
        abort(404)

    data = {
        'title': l.title,
        'route': l.route,
        'author': l.author['author'],
        'email': l.author['email'],
    }
    if request.method == 'POST':
        lines = l.get_result()
        data['labels'] = l.x_axis_value
        data['lines'] = lines

    return render_template('stats/charts/lines.html', data=data)


@bp_stats.route('/period/<tag>', methods=['GET', 'POST'])
def period(tag):
    from ..models.periodic import PeriodicItem
    p = PeriodicItem(tag)
    print(p.get_periods())

    data = []

    return render_template('stats/period.html', data=data)
