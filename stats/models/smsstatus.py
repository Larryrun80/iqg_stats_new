class SmsStatus():
    status = {
        'ACCEPTD': 'API接口消息被接受',
        'DELIVRD': '短信送到手机',
        'EXPIRED': '短信过期',
        'DELETED': '短信被删除',
        'UNDELIV': '无法投递',
        'UNKNOWN': '状态不知',
        'REJECTD': '短信被拒绝',
        'ET:0101': '缺少操作命令',
        'ET:0102': '无效操作命令',
        'ET:0103': '缺少SP的ID',
        'ET:0104': '无效SP的ID',
        'ET:0105': '缺少SP密码',
        'ET:0106': '无效SP密码',
        'ET:0107': '下行源地址被禁止',
        'ET:0108': '无效下行源地址',
        'ET:0109': '缺少下行目的地址',
        'ET:0110': '无效下行目的地址',
        'ET:0111': '超过下行目的地址限制',
        'ET:0112': 'ESM_CLASS被禁止',
        'ET:0113': '无效ESM_CLASS',
        'ET:0114': 'PROTOCOL_ID被禁止',
        'ET:0115': '无效PROTOCOL_ID',
        'ET:0116': '缺少消息编码格式',
        'ET:0117': '无效消息编码格式',
        'ET:0118': '缺少消息内容',
        'ET:0119': '无效消息内容',
        'ET:0120': '无效消息内容长度',
        'ET:0121': '优先级被禁止',
        'ET:0122': '无效优先级',
        'ET:0123': '定时发送时间被禁止',
        'ET:0124': '无效定时发送时间',
        'ET:0125': '有效时间被禁止',
        'ET:0126': '无效有效时间',
        'ET:0127': '通道ID被禁止',
        'ET:0128': '无效通道ID',
        'ET:0131': '缺少批量下行类型',
        'ET:0132': '无效批量下行类型',
        'ET:0133': '无效任务ID',
        'ET:0134': '无效批量下行标题',
        'ET:0135': '缺少批量下行内容',
        'ET:0136': '无效批量下行内容',
        'ET:0137': '缺少批量下行内容URL',
        'ET:0138': '无效批量下行内容URL',
        'ET:0139': 'SP服务代码不存在',
        'ET:0140': '无效的不同内容批量下行地址和内容',
        'ET:0201': 'MSISDN号码段不存在',
        'ET:0202': 'MSISDN号码段停用',
        'ET:0210': 'MSISDN号码被过滤',
        'ET:0220': '内容被过滤',
        'ET:0221': '内容被人工过滤',
        'ET:0226': '内容处理错误',
        'ET:0230': '下行路由失败',
        'ET:0240': '上行路由失败',
        'ET:0250': '配额不足',
        'ET:0251': '没有配额',
        'ET:0261': '超出相同内容流量控制',
        'ET:0262': '超出不同内容流量控制',
        'ET:0263': '超出同一SP服务相同内容流量控制',
        'ET:0264': '有效时间过期',
        'ET:0265': '分拆为长短信',
        'ET:0266': '分拆为分拆短信',
        'ET:0301': 'SP被禁止',
        'ET:0302': 'SP被锁定',
        'ET:0303': '无效IP地址',
        'ET:0304': '超过传输速度限制',
        'ET:0305': '超过传输连接限制',
        'ET:0306': 'SMS下行被禁止',
        'ET:0307': 'SMS批量下行被禁止',
        'ET:0308': 'SMS上行被禁止',
        'ET:0309': 'SMS状态报告被禁止',
        'ET:0310': 'SP上行接口不存在',
        'ET:0311': 'SP状态报告接口不存在',
        'ET:0401': '源号码与通道号不匹配',
        'ET:0402': '下发到运行商网关异常',
        'ET:0403': '下发到运行商网关无反馈',
        'ET:0404': '下发到运行商号码内容重复',
        'ET:0500': '运行商网关反馈信息',
        'ET:0601': 'API接口HttpException',
        'ET:0602': 'API接口IOException',
    }

    codes = {
        '000': '发送成功',
        '100': 'SP API接口参数错误',
        '200': 'MLINK平台内部过滤路由信息',
        '300': 'MLINK平台内部SP配置信息',
        '400': 'MLINK网关发送时错误',
        '500': '运行商反馈的信息',
        '600': 'MLINK API发送时错误',
    }

    success_code = '000'

    @classmethod
    def get_status(cls, code, status_code):
        result = {
            'success': False,
            'info': '',
        }
        if code not in cls.codes.keys():
            result['success'] = False
            result['info'] = '未知状态码 {}'.format(code)
        elif status_code not in cls.status.keys():
            result['success'] = False
            result['info'] = '未知状态 {}'.format(status_code)
        elif code == cls.success_code:
            result['success'] = True
            result['info'] = cls.status[status_code]
        else:
            result['success'] = False
            result['info'] = cls.status[status_code]

        return result
