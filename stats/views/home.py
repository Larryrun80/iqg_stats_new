from flask import (Blueprint,
                   render_template,
                   request,
                   jsonify,
                   url_for)

home = Blueprint('home', __name__)


@home.route('/')
def index():
    return render_template('home/default.html')


@home.route('/test')
def test():
    return render_template('test.html')


@home.route('/export', methods=['POST'])
def export():
    from ..models.export import ExportData
    ed = ExportData(request.form['file_type'], request.form['cid'])
    result = ed.get_file()
    if result['message'] == 'success':
        result['url'] = url_for('static', filename=result['url'])
    return jsonify(result)
