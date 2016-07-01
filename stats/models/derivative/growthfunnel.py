import re

import arrow

from ..funnel import FunnelItem


class GrowthFunnel(FunnelItem):
    def __init__(self):
        funnel_path = '{dir}/{file}'.format(
            dir=self.basedir,
            file=self.app_configs['GROWTH_FUNNEL_PATH'])

        super().__init__('channel', funnel_path)

    def update_source(self, ctype, raw_input):
        base_funnel_item = {
            'id': "base",
            'name': "基准用户",
        }
        post_value = self.get_post_value(ctype, raw_input)
        if ctype == 'userid':
            self.base_ids = {
                'source': 'list',
                'ids': post_value,
            }
            base_funnel_item['source'] = 'list'
            base_funnel_item['code'] = post_value
            self.funnel.insert(0, base_funnel_item)
        if ctype == 'mobile':
            self.base_ids = {
                'source': 'iqg_ro',
                'ids': 'select id from user where mobile in ({})'.format(
                    ', '.join(post_value)),
            }
            base_funnel_item['source'] = 'iqg_ro'
            base_funnel_item['code'] = \
                'select count(0) from user where id in {ids}'
            self.funnel.insert(0, base_funnel_item)
        if ctype == 'coupon':
            self.base_ids = {
                'source': 'iqg_ro',
                'ids': 'select distinct(u.id) from coupon c '
                       'left join user u on c.user_id=u.id '
                       'where c.promo_activity_id={};'.format(post_value)
            }
            base_funnel_item['source'] = 'iqg_ro'
            base_funnel_item['code'] = \
                'select count(0) from user where id in {ids}'
            self.funnel.insert(0, base_funnel_item)

    def get_post_value(self, ctype, raw_input):
        if ctype in ('userid', 'mobile'):
            int_regex = re.compile(r'^\d+$', re.IGNORECASE)
            raw_input = raw_input.replace('\r', ',')
            raw_input = raw_input.replace('\n', ',')
            raw_input = raw_input.replace('，', ',')
            raw_ids = filter(None, raw_input.split(','))
            data = []
            for ri in raw_ids:
                ri = ri.strip()
                if re.match(int_regex, ri):
                    data.append(ri)
            return data
        else:
            return raw_input.strip()

    def get_coupons(self, month_passed=6):
        sql = 'select id, name '\
              'from promo_activity '
        data = []

        if month_passed and isinstance(month_passed, int):
            start_date = arrow.now('Asia/Shanghai')\
                              .replace(months=-month_passed)
            sql += " where created_at>'{0}'"\
                   "".format(start_date.format('YYYY-MM-DD'))
        sql += " order by created_at desc"
        coupon_info = self.get_data('iqg_ro', sql)

        if len(coupon_info['data']) > 0:
            for (coupon_id, coupon_name) in coupon_info['data']:
                coupon_data = {
                    'id': coupon_id,
                    'name': coupon_name
                }
                data.append(coupon_data)
        return data
