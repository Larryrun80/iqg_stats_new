import re

from ..funnel import FunnelItem


class ChannelFunnel(FunnelItem):
    def __init__(self):
        funnel_path = '{dir}/{file}'.format(
            dir=self.basedir,
            file=self.app_configs['CHANNEL_FUNNEL_PATH'])

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

    def get_post_value(self, ctype, raw_input):
        parse_int_types = ('userid', 'mobile')
        int_regex = re.compile(r'^\d+$', re.IGNORECASE)
        raw_input = raw_input.replace('\r', ',')
        raw_input = raw_input.replace('\n', ',')
        raw_input = raw_input.replace('，', ',')
        raw_ids = filter(None, raw_input.split(','))
        data = []
        for ri in raw_ids:
            ri = ri.strip()
            if ctype in parse_int_types \
               and re.match(int_regex, ri):
                data.append(ri)

        return data
