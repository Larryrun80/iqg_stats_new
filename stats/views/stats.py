from urllib.parse import unquote

from flask import (Blueprint,
                   render_template,
                   abort,
                   flash,
                   request)
from flask.ext.paginate import Pagination

bp_stats = Blueprint('stats', __name__)


@bp_stats.route('/query/<tag>')
def query(tag):
    sort_param = request.args.get('sort', None)
    sort_words = None
    if sort_param:
        sort_params = sort_param.split('_')
        sort_words = unquote(unquote(sort_params[0]))
        if sort_params[1] == 'd':
            sort_words += ' DESC'
        elif sort_params[1] == 'a':
            sort_words += ' ASC'

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
    if q.count:
        page_size = request.args.get('pagesize', 20)
    else:
        page_size = 0
    try:
        current_page = int(current_page)
        page_size = int(page_size)
    except:
        flash('page or pagesize is not int')
        return render_template('stats/query.html')
    total = q.get_result_count()

    data['rows'] = q.get_result(page_size=page_size,
                                current_page=current_page,
                                sort=sort_words)
    data['columns'] = q.columns
    if q.sort_cols:
        data['sort_cols'] = q.sort_cols

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
    data = {
        'title': p.title,
        'route': p.route,
        'author': p.author['author'],
        'email': p.author['email'],
    }
    if request.method == 'POST':
        data['data'] = p.assemble_data()

    return render_template('stats/period.html', data=data)


@bp_stats.route('/funnel/<tag>', methods=['GET', 'POST'])
def funnel(tag):
    from ..models.funnel import FunnelItem
    f = FunnelItem(tag)
    data = {
        'title': f.title,
        'route': f.route,
        'author': f.author['author'],
        'email': f.author['email'],
    }
    if request.method == 'POST':
        data['data'] = f.get_funnel_result()

    return render_template('stats/funnel.html', data=data)


@bp_stats.route('/cf', methods=['GET', 'POST'])
def channel_funnel():
    from ..models.derivative.channelfunnel import ChannelFunnel
    cf = ChannelFunnel()
    data = {
        'coupon_info': cf.get_coupons()
    }
    if request.method == 'POST':
        cf.update_source(request.form['channel_type'],
                         request.form['channel_value'])
        result = cf.get_funnel_result()
        if result:
            data['tab'] = request.form['channel_type']
            data['source'] = request.form['channel_value']
            data['result'] = result
        else:
            flash('当前查询没有找到任何用户')
    return render_template('stats/derivative/channel_funnel.html',
                           data=data)


@bp_stats.route('/gf', methods=['GET', 'POST'])
def growth_funnel():
    from ..models.derivative.growthfunnel import GrowthFunnel
    gf = GrowthFunnel()
    data = {
        'coupon_info': gf.get_coupons()
    }
    if request.method == 'POST':
        gf.update_source(request.form['channel_type'],
                         request.form['channel_value'])
        result = gf.get_funnel_result()
        if result:
            data['tab'] = request.form['channel_type']
            data['source'] = request.form['channel_value']
            data['result'] = result
        else:
            flash('当前查询没有找到任何用户')
    return render_template('stats/derivative/growth_funnel.html',
                           data=data)
