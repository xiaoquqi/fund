#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This class if to collect data from https://fund.eastmoney.com/

Use this class you can collect fund info, fund rank and fund addtional
data like Sharpe Ratio on the web page.
"""

from datetime import datetime, timedelta
import json
import logging
import time

import pandas as pd
import requests

BASE_URL = "http://fund.eastmoney.com"
FUND_LIST_URL = "%s/js/fundcode_search.js" % BASE_URL
FUND_RANK_URL = "%s/data/rankhandler.aspx" % BASE_URL
FUND_DETAIL_BASE_URL = "%s/f10" % BASE_URL
RETRY_TIMES = 3

DEFAULT_HEADERS = {
    "Referer": "http://fund.eastmoney.com/data/fundranking.html"
}

FUND_RANK_TITLES = ["code", "基金简称", "基金编码", "日期",
                    "单位净值", "累计净值", "日增长率",
                    "近1周", "近1月", "近3月", "近6月", "近1年",
                    "近2年", "近3年", "今年来", "成立来", "成立时间",
                    "未知字段1", "未知字段2", "原费率", "折扣费率",
                    "未知字段3", "未知字段4", "未知字段5", "未知字段6"]
FUND_TS_TITLES = ["夏普比率近一年", "夏普比率近二年", "夏普比率近三年"]

# 所有-all 股票型-gp 混合型-hh 债券型-zq 指数型-zs 保本型-bb QDII-qdii LOF-lof
FUND_TYPES = ["all", "gp", "hh", "zq", "zs", "bb", "qdii", "lof"]


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
        """Return pandas DataFrame type"""
        funds = []
        page_index = 1
        # if current fund count < page number means no more data
        curr_fund_count = self.pn

        while curr_fund_count >= self.pn:
            funds_in_page = self._get_list(page_index)

            curr_fund_count = len(funds_in_page)
            page_index += 1

            funds += funds_in_page

        fundrank_dt = pd.DataFrame(funds, columns=FUND_RANK_TITLES)
        return fundrank_dt

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

        # NOTE(Ray): After to json, the value is still string, use
        # split convert to real list
        ret_funds = []
        for fund in all_funds:
            ret_funds.append(fund.split(","))
        return ret_funds


class FundInfo(object):

    def __init__(self, fund_codes):
        self.fund_codes = fund_codes

    def list(self):
        """Return pandas DataFrame type"""

        fund_dt = None
        count = 0
        for fund_code in self.fund_codes:
            for i in range(RETRY_TIMES):
                logging.info("Trying %s time(s) to get info..." % i)
                try:
                    fund_info = self._get_info(fund_code)
                    break
                except ConnectionResetError:
                    logging.warning("Get connection reset error, "
                                    "will retry to get... ")
            if count == 0:
                fund_dt = fund_info
            else:
                fund_dt = fund_dt.append(fund_info)
            count = count + 1
            logging.debug("Already got %s count funds." % count)
            time.sleep(0.1)

        return fund_dt

    def _get_info(self, fund_code):
        fund_info_url = "%s/%s.html" % (FUND_DETAIL_BASE_URL,
                                        fund_code)
        logging.debug("Getting date from %s" % fund_info_url)
        tables = pd.read_html(fund_info_url)
        df = tables[1]
        col1_df = df[[0, 1]]
        col2_df = df[[2, 3]]
        col1_df.set_index(0, inplace=True)
        col2_df.set_index(2, inplace=True)
        col1_df = col1_df.T
        col2_df = col2_df.T
        col1_df["code"] = fund_code
        col2_df["code"] = fund_code
        col1_df.set_index("code", inplace=True)
        col2_df.set_index("code", inplace=True)
        info_df = pd.concat([col1_df, col2_df], axis=1)
        return info_df


class FundTsData(object):
    """Retrun fund addtional data like Sharpe Ratio"""

    def __init__(self, fund_codes):
        self.fund_codes = fund_codes

    def list(self):
        """Return pandas DataFrame type"""

        fund_dt = None
        count = 0
        for fund_code in self.fund_codes:
            fund_ts = self._get_ts(fund_code)
            if count == 0:
                fund_dt = fund_ts
            else:
                fund_dt = fund_dt.append(fund_ts)
            count = count + 1
            logging.debug("Already got %s count funds." % count)

        fund_dt.columns = FUND_TS_TITLES
        return fund_dt

    def _get_ts(self, fund_code):
        fund_info_url = "%s/tsdata_%s.html" % (FUND_DETAIL_BASE_URL,
                                               fund_code)
        logging.debug("Getting date from %s" % fund_info_url)
        tables = pd.read_html(fund_info_url)
        df = tables[1]
        df["code"] = fund_code
        df.set_index("code", inplace=True)
        df.drop(u"基金风险指标", axis="columns", inplace=True)
        info_df = df[1:]
        return info_df


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
