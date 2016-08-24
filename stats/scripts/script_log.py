import arrow


def print_log(message, m_type='INFO'):
    m_types = ('INFO', 'WARNING', 'ERROR')
    prefix = '[ {} ]'.format(arrow.now().format('YYYY-MM-DD HH:mm:ss:SSS'))
    if str(m_type).upper() in m_types:
        m_type = str(m_type).upper()
    else:
        raise RuntimeError('Invalid log type: {}'.format(m_type))

    print('{} -{}- {}'.format(prefix, m_type, message))
