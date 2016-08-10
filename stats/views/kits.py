from flask import (Blueprint,
                   render_template,
                   request,
                   jsonify,
                   url_for)

kits = Blueprint('kits', __name__)


@kits.route('/get_delivery_template', methods=['POST'])
def get_delivery_template():
    '''get_delivery_template

        Get delivery template from mid
        for modal display
    '''
    result = {'success': False, 'title': '运费模版'}
    from ..models.kits import Kits
    mid = request.form['mid']
    try:
        int(mid)
    except:
        return jsonify(result)

    delivery_info = Kits().get_delivery_template(int(mid))
    if delivery_info:
        result['success'] = True
        result['type'] = 'table'
        result['info'] = delivery_info
    return jsonify(result)
