import arrow

from ..stats_base import StatsBase


class HsqDaily(StatsBase):
    def __init__(self):
        pass

    def get_table_data(self):
        table_data = []
        platforms = ['group_app', 'jx', 'app_iOS', 'app_android', 'app_h5']
        stats_date = arrow.now('Asia/Shanghai').replace(days=-1)
        cols = None

        for p in platforms:
            # 昨日数值
            sql = '''
                    select concat(platform, ' [{sdate}]') 业务,
                    uv, active_uv 有效UV,
                    customer_normal_cnt+customer_ws_cnt 顾客,
                    new_customer_normal_cnt+new_customer_ws_cnt 新客,
                    order_cnt 订单数,
                    round(order_cnt/active_uv*100,1) 下单转化,
                    round(gmv,0) gmv,
                    if(order_cnt>0,round(gmv/order_cnt,2),0) 客单,
                    round(gross_profit,0) 营收,
                    round(net_profit,0) 利润
                    from hsq_daily_statistic_new
                    where `date`='{sdate}'
                    and platform='{platform}'
            '''.format(sdate=stats_date.format('YYYY-MM-DD'),
                       platform=p)

            data = self.get_mysql_result('', sql)

            if not cols:
                cols = [data['columns']]

            if data['data'][0][0]:
                yesterday_data = list(data['data'][0])

            # avg of last 7 days
            sql_last7d = '''
                    select concat(platform, ' [7日均值]'),
                    round(avg(uv),0),
                    round(avg(active_uv),0),
                    round(avg(customer_normal_cnt+customer_ws_cnt),0),
                    round(avg(new_customer_normal_cnt+new_customer_ws_cnt),0),
                    round(avg(order_cnt),0),
                    round(avg(order_cnt)/avg(active_uv)*100,1),
                    round(avg(gmv),0),
                    if(order_cnt,round(avg(gmv)/avg(order_cnt),2),0),
                    round(avg(gross_profit),0),
                    round(avg(net_profit),0)
                    from hsq_daily_statistic_new
                    where `date`>'{sdate}'
                    and `date`<'{edate}'
                    and platform='{platform}'
            '''.format(edate=stats_date.format('YYYY-MM-DD'),
                       sdate=stats_date.replace(days=-9).format('YYYY-MM-DD'),
                       platform=p)
            data = self.get_mysql_result('', sql_last7d)
            if data['data'][0][0]:
                last7d_data = list(data['data'][0])

            # ratio
            ratio_row = ['{} [增幅]'.format(p), ]
            for i in range(1, len(yesterday_data)):
                if yesterday_data[i]:
                    yd = float(yesterday_data[i])
                else:
                    yd = 0
                if last7d_data[i]:
                    l7d = float(last7d_data[i])
                else:
                    l7d = 0

                if l7d != 0:
                    ratio_row.append('{} %'.format(
                        round((yd - l7d) / abs(l7d) * 100, 2)))
                else:
                    ratio_row.append('-')

            name = {
                'group_app': '拼团',
                'jx': '分销',
            }

            if p in name.keys():
                yesterday_data[0] = yesterday_data[0].replace(p, name[p])
                last7d_data[0] = last7d_data[0].replace(p, name[p])
                ratio_row[0] = ratio_row[0].replace(p, name[p])

            percent_cols = (6,)
            for pc in percent_cols:
                yesterday_data[pc] = '{} %'.format(yesterday_data[pc])
                last7d_data[pc] = '{} %'.format(last7d_data[pc])

            table_data.append(yesterday_data)
            table_data.append(last7d_data)
            table_data.append(ratio_row)

        return {'head': cols, 'body': table_data,
                'title': '好食期运营指标',
                'author': 'Larry', 'email': 'guonan@doweidu.com'}
