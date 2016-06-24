import re

from ... import app
from ..funnel import FunnelItem


class ChannelFunnel(FunnelItem):
    funnel_path = '{dir}/{file}'.format(
        dir=app.basedir,
        file=app.config['CHANNEL_FUNNEL_PATH'])

    def __init__(self):
        super().__init__('channel')

    def update_source(self):
        pass

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
