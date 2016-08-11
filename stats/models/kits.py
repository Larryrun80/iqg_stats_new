from .stats_base import StatsBase


class Kits(StatsBase):
    def get_delivery_template(self, mid):
        if not isinstance(mid, int):
            return None

        sql = '''
                select      group_concat(dest) 城市,
                            round(base_price/100, 2) 首重,
                            round(extra_unit_price/100, 2) 续重
                from        delivery_template
                where       merchant_id={mid}
                group by    concat(base_price, ',', extra_unit_price)
                order by    base_price
              '''.format(mid=mid)

        return self.get_mysql_result('hsq_ro', sql)
