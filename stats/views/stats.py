import re
from urllib.parse import unquote

import arrow
from flask import (Blueprint,
                   render_template,
                   abort,
                   flash,
                   request)
from flask.ext.login import (login_required,
                             logout_user,
                             login_user,
                             current_user)
from flask.ext.paginate import Pagination

bp_stats = Blueprint('stats', __name__)


@bp_stats.route('/query/<tag>', methods=['GET', 'POST'])
def query(tag):
    # start query
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
        'params': q.params,
    }
    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

    # params
    param_regex = re.compile(r'\{\w+\}', re.IGNORECASE)
    params = re.findall(param_regex, q.code)

    if params:
        param_check = True
        if not q.params:
            param_check = False
        else:
            for p in params:
                if p[1:-1] not in q.params.keys():  # 如果yaml中没有定义param
                    param_check = False
                    break
                else:  # 开始处理参数
                    val = None
                    yp = q.params[p[1:-1]]
                    if 'default' in yp.keys():
                        val = yp['default']
                    rp = request.args.get(p[1:-1], None)
                    if rp:
                        val = rp
                    if val is not None:
                        if yp['type'] == 'date':
                            if val == 'yesterday':
                                val = arrow.now().replace(days=-1)\
                                                 .format('YYYY-MM-DD')
                            if val == 'today':
                                val = arrow.now().format('YYYY-MM-DD')
                        elif yp['type'] in ('int', 'float'):
                            try:
                                float(val)
                            except:
                                flash('参数类型不符')
                                return render_template('stats/query.html',
                                                       data=data)

                        q.code = q.code.replace(p, val)
            if not param_check:
                flash('缺少参数或参数有问题')
                return render_template('stats/query.html', data=data)

    # sort part, if client ask for sort
    sort_param = request.args.get('sort', None)
    sort_words = None
    if sort_param:
        sort_params = sort_param.split('_')
        if len(sort_params) == 2 and sort_params[1] in ('a', 'd'):
            sort_words = unquote(unquote(sort_params[0]))
            if sort_params[1] == 'd':
                sort_words += ' DESC'
            elif sort_params[1] == 'a':
                sort_words += ' ASC'
            data['sortqs'] = sort_param

    # filters
    if q.filters:
        filter_param = request.args.get('filter', None)
        if filter_param:
            filter_items = unquote(unquote(filter_param)).split('&')
            filter_query = {}
            for item in filter_items:
                if item:
                    item_kv = item.split('=')
                    if len(item_kv) == 2:
                        filter_query[item_kv[0]] = item_kv[1]

            if filter_query:
                f_code = 'select * from ({})t where '.format(q.code)
                clauses = []
                for f in q.filters:
                    if f['id'] in filter_query.keys():
                        filter_query[f['id']] = \
                            str(filter_query[f['id']]).strip()
                    if f['type'] == 'str' and f['id'] in filter_query.keys():
                        clauses.append('{field} like "%{value}%"'.format(
                            field=f['name'], value=filter_query[f['id']]))
                    if f['type'] == 'float':
                        if ('{}_min'.format(f['id']) in filter_query.keys()):
                            clauses.append('{field} >= {value}'.format(
                                field=f['name'],
                                value=filter_query['{}_min'.format(f['id'])]))
                        if ('{}_max'.format(f['id']) in filter_query.keys()):
                            clauses.append('{field} <= {value}'.format(
                                field=f['name'],
                                value=filter_query['{}_max'.format(f['id'])]))
                    if f['type'] == 'date':
                        if ('{}_early'.format(f['id']) in filter_query.keys()):
                            clauses.append('{field} >= "{value}"'.format(
                                field=f['name'],
                                value=filter_query[
                                    '{}_early'.format(f['id'])]))
                        if ('{}_late'.format(f['id']) in filter_query.keys()):
                            clauses.append('{field} <= "{value}"'.format(
                                field=f['name'],
                                value=filter_query['{}_late'.format(f['id'])]))
                if clauses:
                    f_code = f_code + ' and '.join(clauses)
                    q.code = f_code
        data['filters'] = q.filters

    current_page = request.args.get('page', 1)
    if q.paging:
        page_size = request.args.get('pagesize', 20)
    else:
        page_size = 0
    try:
        current_page = int(current_page)
        page_size = int(page_size)
    except:
        flash('page or pagesize is not int')
        return render_template('stats/query.html')
    q.count = 'select count(0) from ({})t'.format(q.code)
    total = q.get_result_count()

    data['rows'] = q.get_result(page_size=page_size,
                                current_page=current_page,
                                sort=sort_words)

    # total = len(q.get_result())
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

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

    if request.method == 'POST':
        lines = l.get_result()
        data['labels'] = l.x_axis_value
        data['lines'] = lines

    return render_template('stats/charts/lines.html', data=data)


@bp_stats.route('/highline/<tag>', methods=['GET', 'POST'])
def highline(tag):
    from ..models.charts.highlines import HighLineItem
    l = HighLineItem(tag)
    if not l.title:
        abort(404)

    data = {
        'title': l.title,
        'route': l.route,
        'author': l.author['author'],
        'email': l.author['email'],
    }

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

    if request.method == 'POST':
        lines = l.get_result()
        data['labels'] = l.x_axis_value
        data['lines'] = lines
        data['y_axis'] = l.y_axis

    return render_template('stats/charts/highlines.html', data=data)


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

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

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

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

    if request.method == 'POST':
        data['data'] = f.get_funnel_result()

    return render_template('stats/funnel.html', data=data)


@bp_stats.route('/cf', methods=['GET', 'POST'])
def channel_funnel():
    from ..models.derivative.channelfunnel import ChannelFunnel
    cf = ChannelFunnel()
    data = {
        'title': cf.title,
        'route': cf.route,
        'author': cf.author['author'],
        'email': cf.author['email'],
        'coupon_info': cf.get_coupons()
    }

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

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
        'title': gf.title,
        'route': gf.route,
        'author': gf.author['author'],
        'email': gf.author['email'],
        'coupon_info': gf.get_coupons()
    }

    if current_user:
        user = current_user
        if user.id:
            data['is_favourite'] = user.is_favourite(request.path)

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
