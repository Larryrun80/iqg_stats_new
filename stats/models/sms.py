import requests

from .error import AppError
from .smsstatus import SmsStatus
from .. import app


class Sms():
    country_code = '86'
    conf = ('request_prifix', 'spid', 'pwd')
    result_params = ('command', 'spid', 'mtstat', 'mterrcode')

    def __init__(self, recv=[], content=''):
        self.setRecv(recv)
        self.setContent(content)
        self.sms_conf = {}

        for c in self.conf:
            conf_key = 'SMS_{}'.format(c.upper())
            if conf_key in app.config.keys():
                self.sms_conf[c] = app.config[conf_key]
            else:
                raise AppError('MISSING_PARAM', param=conf_key)

    def setRecv(self, recv):
        if not recv:
            self.recv = None
        elif not isinstance(recv, list):
            raise AppError('TYPE_ERROR', param='recv', expect_type='list')
        else:
            self.recv = (self.country_code + str(r) for r in recv)

    def setContent(self, content):
        if not content:
            self.content = None
        elif not isinstance(content, str):
            raise AppError('TYPE_ERROR', param='content', expect_type='string')
        else:
            self.content = content

    def sendSms(self):
        if not self.recv:
            raise AppError('NO_ATTR', attr='receivers', obj='SMS')
        if not self.content:
            raise AppError('NO_ATTR', attr='content', obj='SMS')

        payload = {
            'command': 'MULTI_MT_REQUEST',
            'spid': self.sms_conf['spid'],
            'sppassword': self.sms_conf['pwd'],
            'das': ','.join(self.recv),
            'dc': '15',  # using gbk
            'sm': self.content.encode('GBK').hex(),
        }

        r = requests.get(self.sms_conf['request_prifix'], params=payload)
        self.deal_send_status(r)

    def deal_send_status(self, resp):
        if resp.status_code == requests.codes.ok:
            response = {}
            raw_resp = resp.text.split('&')
            for r in raw_resp:
                kv = r.split('=')
                if len(kv) == 2:
                    response[kv[0]] = kv[1]

            for rp in self.result_params:
                if rp not in response.keys():
                    message = '{} not in response ||| '.format(rp)
                    message += 'method: {method} | '\
                              'url: {url} | '\
                              'request_header: {header} | '\
                              'status_code: {code} | '\
                              'response: {resp_text}'.format(
                                    method=resp.request.method,
                                    url=resp.url,
                                    header=resp.request.headers,
                                    code=resp.status_code,
                                    resp_text=resp.text
                                )
                    raise AppError('NOT_VALID_RESPONSE', info=message)

            status = SmsStatus().get_status(response['mterrcode'],
                                            response['mtstat'])

            if False == status['success']:
                raise AppError('SMS_ERROR', info=status['info'])
        else:
            r.raise_for_status()
