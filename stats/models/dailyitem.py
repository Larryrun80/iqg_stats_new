import arrow

from .stats_base import StatsBase


class DailyItemCollector(StatsBase):
    def __init__(self, date=None):
        if not date:
            self.date = arrow.now('Asia/Shanghai')
        else:
            self.date = arrow.get(date)

    def get_items(self, date=None):
        if not date:
            date = self.date

        code = 'iqg_prod.hsq_daily_items.find({"date": "'\
            + date.format('YYYY-MM-DD') \
            + '"})'

        result = self.get_mongo_result('iqg_mongo', code)[0]
        return result

    def get_diff(self, date=None):
        if not date:
            date = self.date
        before_date = date.replace(days=-1)

        after = self.get_items(date)
        before = self.get_items(before_date)

        after_ids = [i['item_id'] for i in after['item_data']]
        before_ids = [i['item_id'] for i in before['item_data']]

        result = {'online': [], 'offline': []}
        result['info'] = "{} [ {} ]   &   {} [ {} ]"\
                         "".format(after['item_count'],
                                   date.format('MM-DD'),
                                   before['item_count'],
                                   before_date.format('MM-DD'))

        online = set(after_ids) - set(before_ids)
        offline = set(before_ids) - set(after_ids)

        for item in after['item_data']:
            if item['item_id'] in online:
                result['online'].append(item)

        for item in before['item_data']:
            if item['item_id'] in offline:
                result['offline'].append(item)

        return result
