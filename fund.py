# !/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import json
import logging

import requests

BASE_URL = "http://fund.eastmoney.com"
FUND_LIST_URL = "%s/js/fundcode_search.js" % BASE_URL
FUND_RANK_URL = "%s/data/rankhandler.aspx" % BASE_URL

DEFAULT_HEADERS = {
    "Referer": "http://fund.eastmoney.com/data/fundranking.html"
}

FUND_RANK_TITLES = ["基金代码", "基金简称", "基金编码", "日期",
                    "单位净值", "累计净值", "日增长率",
                    "近1周", "近1月", "近3月", "近6月", "近1年",
                    "近2年", "近3年", "今年来", "成立来", "成立时间",
                    "未知字段1", "未知字段2", "原费率", "折扣费率",
                    "未知字段3", "未知字段4", "未知字段5", "未知字段6"]

# 所有-all 股票型-gp 混合型-hh 债券型-zq 指数型-zs 保本型-bb QDII-qdii LOF-lof
FUND_TYPES = ["all", "gp", "hh", "zq", "zs", "bb", "qdii", "lof"]


class DTWrapper(object):
    """Datetime wrapper"""

    @classmethod
    def today_date(self):
        return self._short_datetime(self._today())

    @classmethod
    def delta_today(self, delta):
        return self._short_datetime(
            self._cal_pre_date(self._today(), delta)
        )

    @classmethod
    def _full_datetime(self, dt):
        return datetime.strftime(dt, "%Y-%m-%d 00:00:00")

    @classmethod
    def _short_datetime(self, dt):
        return datetime.strftime(dt, "%Y-%m-%d")

    @classmethod
    def _cal_pre_date(self, start_date, delta):
        return start_date - timedelta(seconds=self._to_seconds(delta))

    @classmethod
    def _today(self):
        return datetime.now()

    @classmethod
    def _to_seconds(self, delta):
        """Convert delta to seconds"""
        length = len(delta)
        unit = delta[length - 1:]
        num = delta[:length - 1]

        try:
            num = int(num)
        except Exception:
            raise Exception("Invalid delta format")
        if unit not in ['h', 'd', 'm', 'y']:
            raise Exception("Invalid delta format")
        if unit == 'h':
            return num * 60 * 60
        elif unit == 'd':
            return num * 60 * 60 * 24
        elif unit == 'm':
            return num * 60 * 60 * 24 * 30
        elif unit == 'y':
            return num * 60 * 60 * 24 * 365
        else:
            raise Exception("Invalid delta fromat")


class Fund(object):

    def list(self):
        """Return all fund list"""

        try:
            logging.info("Trying to get fund list "
                         "from %s..." % FUND_LIST_URL)
            req = requests.get(FUND_LIST_URL)
            all_funds_txt = req.text
        except Exception as e:
            logging.error("Get fund list from %s "
                          "failed." % FUND_LIST_URL)
            raise e

        logging.info("Get fund list from %s "
                     "successfully." % FUND_LIST_URL)
        logging.debug("Fund list %s returns: %s" % (
            FUND_LIST_URL, all_funds_txt))

        all_funds_txt = all_funds_txt[
            all_funds_txt.find('=') + 2:all_funds_txt.rfind(';')]
        all_funds = json.loads(all_funds_txt)

        return all_funds


class FundRank(object):
    """Get fund list and history rank"""

    def __init__(self, ft="all", pn=1000, period="1y"):
        """Intialization method

        ft: fund type
        pn: page number
        period: search rank before given period
        """

        if ft not in FUND_TYPES:
            raise Exception("Fund type is not supported, support "
                            "fund types: %s" % FUND_TYPES)

        self.ft = ft
        self.pn = pn
        self.period = period
        self.start_date = DTWrapper.today_date()
        self.end_date = DTWrapper.delta_today(period)

    def list(self):

        funds = []
        page_index = 1
        # if current fund count < page number means no more data
        curr_fund_count = self.pn

        while curr_fund_count >= self.pn:
            funds_in_page = self._get_list(page_index)

            curr_fund_count = len(funds_in_page)
            page_index += 1

            funds += funds_in_page

        funds.insert(0, ",".join(FUND_RANK_TITLES))
        return funds

    def _get_list(self, page_index):
        sc = "1nzf"
        params = {
            "op": "ph",
            "dt": "kf",
            "ft": self.ft,
            "rs": "",
            "gs": 0,
            "sc": sc,
            "st": "desc",
            "sd": self.start_date,
            "ed": self.end_date,
            "qdii": "",
            "tabSubtype": ",,,,,",
            "pi": page_index,
            "pn": self.pn,
            "dx": 1
        }
        logging.info("Trying to get fund rank list "
                     "from %s..." % FUND_RANK_URL)
        try:
            req = requests.get(FUND_RANK_URL,
                               headers=DEFAULT_HEADERS,
                               params=params)
        except Exception as e:
            logging.error("Get rank list from %s "
                          "failed." % FUND_RANK_URL)
            raise e

        all_rank_txt = req.text
        all_rank_txt = all_rank_txt[
            all_rank_txt.find('["'):all_rank_txt.rfind('"]') + 2]
        all_funds = json.loads(all_rank_txt)
        return all_funds
