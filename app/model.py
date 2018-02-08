from app.ext import db
from itertools import product
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declared_attr, AbstractConcreteBase
import pandas as pd


def relation(name):
    for prefix, suffix in product(['Month', 'Week'], ['Sale', 'Book', 'Sold']):
        yield db.relationship(prefix + suffix, backref=name)


class Acreage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    acreage = db.Column(db.String, name='面积段')
    acreage_low = db.Column(db.Integer)
    acreage_high = db.Column(db.Integer)
    month_sale, month_book, month_sold, week_sale, week_book, week_sold = relation('acreage')

    def __repr__(self):
        return f'<Acreage {self.acreage}>'


class Aveprice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    aveprice = db.Column(db.String, name='单价段')
    aveprice_low = db.Column(db.Integer)
    aveprice_high = db.Column(db.Integer)
    month_sale, month_book, month_sold, week_sale, week_book, week_sold = relation('aveprice')

    def __repr__(self):
        return f'<Aveprice {self.aveprice}>'


class Tprice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tprice = db.Column(db.String, name='总价段')
    tprice_low = db.Column(db.Integer)
    tprice_high = db.Column(db.Integer)
    month_sale, month_book, month_sold, week_sale, week_book, week_sold = relation('tprice')

    def __repr__(self):
        return f'<Tprice {self.tprice}>'


class Type(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type_ = db.Column(db.String, name='套型')
    month_sale, month_book, month_sold, week_sale, week_book, week_sold = relation('type_')

    def __repr__(self):
        return f'<Type {self.type_}>'


class MarketBase(AbstractConcreteBase, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    # 地区
    dist = db.Column(db.String, name='区属')
    plate = db.Column(db.String, name='板块')
    zone = db.Column(db.String, name='片区')
    # 功能
    usage = db.Column(db.String, name='功能')
    # 量价
    set_ = db.Column(db.Integer, name='件数')
    space = db.Column(db.Float, name='面积')
    price = db.Column(db.Integer, name='均价')
    money = db.Column(db.Float, name='金额')
    # 项目基本信息
    proj_id = db.Column(db.Integer, name='prjid')
    proj_name = db.Column(db.String, name='projectname')
    pop_name = db.Column(db.String, name='popularizename')
    # 许可证
    permit_id = db.Column(db.Integer, name='permitid')
    permit_no = db.Column(db.String, name='permitno')
    permit_date = db.Column(db.Date, name='perdate')
    # 暂时用不到
    update_time = db.Column(db.String)
    presaleid = db.Column(db.String)

    @declared_attr
    def acreage_id(self):
        return db.Column(db.Integer, db.ForeignKey('acreage.id'), name='面积段')

    @declared_attr
    def aveprice_id(self):
        return db.Column(db.Integer, db.ForeignKey('aveprice.id'), name='单价段')

    @declared_attr
    def tprice_id(self):
        return db.Column(db.Integer, db.ForeignKey('tprice.id'), name='总价段')

    @declared_attr
    def type_id(self):
        return db.Column(db.Integer, db.ForeignKey('type.id'), name='套型')

    @declared_attr
    def __mapper_args__(cls):
        return {'concrete': True} if cls.__name__ != "MarketBase" else {}

    def filter(self, date_range, usage, plate):
        table = self.__class__

        result = self.query.filter(
            table.date.between(*date_range),
            table.usage.in_(usage),
            table.plate.in_(plate)
        )

        return BaseQuery(result, table)

    # TODO: 写入gxj\rank\cut等方法

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.id}>'


class MonthSale(MarketBase):
    date = db.Column(db.Date, name='年月')


class MonthBook(MarketBase):
    date = db.Column(db.Date, name='年月')


class MonthSold(MarketBase):
    date = db.Column(db.Date, name='年月')


class WeekSale(MarketBase):
    date = db.Column(db.Integer, name='星期')
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


class WeekBook(MarketBase):
    date = db.Column(db.Integer, name='星期')
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


class WeekSold(MarketBase):
    date = db.Column(db.Integer, name='星期')
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


class BaseQuery:
    def __init__(self, raw, table):
        self.raw = raw
        self.table = table

    def filter(self, *args):
        res = self.raw.filter(*args)
        return BaseQuery(res, self.table)

    def gourp_by(self, *args):
        res = self.raw.group_by(*args)
        return BaseQuery(res, self.table)

    def group(self, by, outputs):
        col = lambda x: getattr(self.table, x)

        by_fields = [col(x) for x in by]
        outputs_fields = [col(x).label(x) for x in by]
        outputs_fields.extend([func.sum(col(x)).label(x) for x in outputs])

        res = self.raw.from_self(*outputs_fields).group_by(*by_fields)

        return BaseQuery(res, self.table)

    def df(self, index=None, column=None):
        _df = pd.read_sql(self.raw.statement, self.raw.session.bind)
        _df = _df.replace('仙西', '仙林')

        if column:
            return _df.pivot(index, column)
        elif index:
            return _df.set_index(index)
        else:
            return _df
