# __author__ = "WuZhiHu"
# Email: 987704751@qq.com
"""
榜单计算
"""
import db
import datetime
from lib import *
import numpy as np
import json
import math
import time
import copy


ar_4_1_list = ""


def ar_4_1(**kwargs):
    """
    剥离金融股
    :param kwargs:
    :return:
    """
    ret_list = []
    global ar_4_1_list
    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        sql = f"select rp_85 from mc_data_reportrangeformula where comp_code='{comp_code}' " \
              f"order by report_range desc limit 1"
        data = db.query_db(sql)
        rp_85 = data[0][0]
        if rp_85 == 1:
            ret_list.append(comp_code)
    ar_4_1_list = ret_list
    return ret_list


def ar_40(**kwargs):
    """
    合并数据需要
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs['ef_list']:
        ret_dict[i['comp_code']] = []
    return ret_dict


def ar_41_2(**kwargs):
    """
    当天的wd_5和wd_1
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["wd_5_data"]:
        ret_dict[i["comp_code"]] = {"wd_5": i["wd_5"], 'wd_1': i['wd_1']}
    return ret_dict


def ar_41_3(**kwargs):
    """
    流通股
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ltg_data"]:
        ret_dict[i["comp_code"]] = i["LIQSHARE"]
    return ret_dict


def ar_41_4(**kwargs):
    """
    获取当天的cd_21,cd_22,cd_18
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        ret_dict[i["comp_code"]] = {"cd_21": i["cd_21"], "cd_22": i["cd_22"], "cd_18": i["cd_18"]}
    return ret_dict


def ar_41_5(**kwargs):
    """
    获取上一次资金青睐的输出
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_41"]
    return ret_dict


def ar_41_6(**kwargs):
    """
    判断当天是否为北向资金交易日
    :param kwargs:
    :return:
    """
    cd_21_value_list = []
    cd_17_value_list = []

    for i in kwargs["ef_list"]:
        cd_21 = i["cd_21"]
        cd_17 = i['cd_17']
        if cd_21:
            cd_21_value_list.append(cd_21)
        if cd_17:
            cd_17_value_list.append(cd_17)
    if len(cd_21_value_list) < 100 or len(cd_17_value_list) == 0:
        return 1
    else:
        return 0


ar_41_dict = {}


def ar_41(**kwargs):
    """
    判断资金青睐输出
    :param kwargs:
    :return:
    """
    global ar_41_dict
    ret_dict = {}
    ar_41_3_data = ar_41_3(ltg_data=kwargs["ltg_data"])
    ar_41_4_data = ar_41_4(ef_list=kwargs["ef_list"])
    ar_41_5_data = ar_41_5(piaochi_data=kwargs["piaochi_data"])
    ar_41_2_data = ar_41_2(wd_5_data=kwargs['wd_5_data'])
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])

    print(ar_41_6_data)
    if ar_41_6_data:
        ar_41_dict = copy.deepcopy(ar_41_5_data)
        return ar_41_5_data

    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        gu = i['cd_21']
        je = i['cd_22']
        gushu = i['cd_18']
        if comp_code in ar_41_2_data:
            wd_1 = ar_41_2_data[comp_code]['wd_1']
        else:
            wd_1 = 0

        ltg = ar_41_3_data[i["comp_code"]]

        if comp_code in ar_41_5_data and ar_41_5_data[comp_code] is not None:
            zr_data = json.loads(ar_41_5_data[comp_code])
            if zr_data is None:
                if je > 0:
                    sum_gu = gushu
                    sum_je = je
                    bs_day = 1
                else:
                    sum_gu = 0
                    sum_je = 0
                    bs_day = 0

                if wd_1 > 0:
                    wd_day = 1
                    sum_wd_1 = wd_1
                else:
                    wd_day = 0
                    sum_wd_1 = 0

                if gu > 0.005 or je > 100000000:
                    ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                           "line_2": "北上资金",
                                                           "line_3": "突击大额净买入",
                                                           "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))}, ensure_ascii=False)
                else:
                    ret_dict[i["comp_code"]] = json.dumps({"line_1": "",
                                                           "line_2": "",
                                                           "line_3": "",
                                                           "backup": (3, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))}, ensure_ascii=False)
            elif zr_data["backup"][3] < 2:
                if je > 0:
                    sum_gu = gushu + zr_data["backup"][1]
                    sum_je = je + zr_data["backup"][2]
                    bs_day = 1 + zr_data["backup"][3]
                else:
                    sum_gu = 0
                    sum_je = 0
                    bs_day = 0

                if wd_1 > 0:
                    wd_day = 1 + zr_data["backup"][4]
                    sum_wd_1 = wd_1 + zr_data["backup"][5]
                else:
                    wd_day = 0
                    sum_wd_1 = 0

                if wd_day >= 5:
                    if gu > 0.005 or je > 100000000:
                        ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                               "line_2": "北上资金",
                                                               "line_3": "突击大额净买入",
                                                               "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                              ensure_ascii=False)
                    else:
                        ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(sum_wd_1 / 10000,2)}亿",
                                                               "line_2": "整体资金",
                                                               "line_3": f"连续{wd_day}日净买入",
                                                               "backup": (2, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(sum_wd_1 / 10000,2))},
                                                              ensure_ascii=False)
                else:
                    if gu > 0.005 or je > 100000000:
                        ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                               "line_2": "北上资金",
                                                               "line_3": "突击大额净买入",
                                                               "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                              ensure_ascii=False)
                    else:
                        ret_dict[i["comp_code"]] = json.dumps({"line_1": "",
                                                               "line_2": "",
                                                               "line_3": "",
                                                               "backup": (3, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                              ensure_ascii=False)
            else:
                if wd_1 > 0:
                    wd_day = 1 + zr_data["backup"][4]
                    sum_wd_1 = wd_1 + zr_data["backup"][5]
                else:
                    wd_day = 0
                    sum_wd_1 = 0

                if je > 0:
                    sum_gu = gushu + zr_data["backup"][1]
                    sum_je = je + zr_data["backup"][2]
                    bs_day = 1 + zr_data["backup"][3]
                    sum_ltg_bl = sum_gu / ltg

                    if wd_day >= 5:
                        if (sum_ltg_bl > 0.01) or sum_je > 200000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(sum_je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": f"连续{bs_day}日净买入",
                                                                   "backup": (0, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(sum_je / 100000000,2))},
                                                                  ensure_ascii=False)
                        elif gu > 0.005 or je > 100000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": "突击大额净买入",
                                                                   "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
                        else:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(sum_wd_1 / 10000,2)}亿",
                                                                   "line_2": "整体资金",
                                                                   "line_3": f"连续{wd_day}日净买入",
                                                                   "backup": (
                                                                   2, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(sum_wd_1 / 10000,2))},
                                                                  ensure_ascii=False)
                    else:
                        if (sum_gu / ltg > 0.01) or sum_je > 200000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(sum_je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": f"连续{bs_day}日净买入",
                                                                   "backup": (0, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(sum_je / 100000000,2))},
                                                                  ensure_ascii=False)
                        elif gu > 0.005 or je > 100000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": "突击大额净买入",
                                                                   "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
                        else:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": "",
                                                                   "line_2": "",
                                                                   "line_3": "",
                                                                   "backup": (
                                                                   3, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
                else:
                    sum_gu = 0
                    sum_je = 0
                    bs_day = 0
                    if wd_day >= 5:
                        if gu > 0.005 or je > 100000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": "突击大额净买入",
                                                                   "backup": (
                                                                   1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
                        else:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(sum_wd_1 / 10000,2)}亿",
                                                                   "line_2": "整体资金",
                                                                   "line_3": f"连续{wd_day}日净买入",
                                                                   "backup": (
                                                                   2, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(sum_wd_1 / 10000,2))},
                                                                  ensure_ascii=False)
                    else:
                        if gu > 0.005 or je > 100000000:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                                   "line_2": "北上资金",
                                                                   "line_3": "突击大额净买入",
                                                                   "backup": (
                                                                   1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
                        else:
                            ret_dict[i["comp_code"]] = json.dumps({"line_1": "",
                                                                   "line_2": "",
                                                                   "line_3": "",
                                                                   "backup": (
                                                                   3, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                                  ensure_ascii=False)
        else:
            if je > 0:
                sum_gu = gushu
                sum_je = je
                bs_day = 1
            else:
                sum_gu = 0
                sum_je = 0
                bs_day = 0

            if wd_1 > 0:
                wd_day = 1
                sum_wd_1 = wd_1
            else:
                wd_day = 0
                sum_wd_1 = 0

            if gu > 0.005 or je > 100000000:
                ret_dict[i["comp_code"]] = json.dumps({"line_1": f"{round(je / 100000000,2)}亿",
                                                       "line_2": "北上资金",
                                                       "line_3": "突击大额净买入",
                                                       "backup": (1, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                      ensure_ascii=False)
            else:
                ret_dict[i["comp_code"]] = json.dumps({"line_1": "",
                                                       "line_2": "",
                                                       "line_3": "",
                                                       "backup": (3, sum_gu, sum_je, bs_day, wd_day, sum_wd_1, round(je / 100000000,2))},
                                                      ensure_ascii=False)
    ar_41_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_42_1(**kwargs):
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_42"]
    return ret_dict


ar_42_dict = {}
def ar_42(**kwargs):
    """
    判断本次资金青睐类型(用于列表排序)
    :param kwargs:
    :return:
    """
    global ar_42_dict
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])
    ar_42_1_data = ar_42_1(piaochi_data=kwargs["piaochi_data"])

    if ar_41_6_data:
        ar_42_dict = copy.deepcopy(ar_42_1_data)
        return ar_42_1_data

    ret_dict = {}
    for i in ar_41_dict:
        data = json.loads(ar_41_dict[i])
        if data['backup'][0] >= 2:
            ret_dict[i] = json.dumps({})
        elif data['backup'][0] == 0:
            ret_dict[i] = json.dumps({'type': 0,
                                      'scale': data['backup'][6]})
        elif data['backup'][0] == 1:
            ret_dict[i] = json.dumps({'type': 1,
                                      'scale': data['backup'][6]})
    ar_42_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_43_1(**kwargs):
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_43"]
    return ret_dict


ar_43_dict = {}

def ar_43(**kwargs):
    """
    是否上榜资金青睐
    0上榜，1没上榜
    :param kwargs:
    :return:
    """
    global ar_43_dict
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])
    ar_43_1_data = ar_43_1(piaochi_data=kwargs["piaochi_data"])

    if ar_41_6_data:
        ar_43_dict = copy.deepcopy(ar_43_1_data)
        return ar_43_1_data

    ret_dict = {}
    for i in ar_41_dict:
        data = json.loads(ar_41_dict[i])
        if data['backup'][0] >= 2:
            ret_dict[i] = 1
        else:
            ret_dict[i] = 0
    ar_43_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_44_1(**kwargs):
    """
    整理最近9天上榜情况
    :param kwargs:
    :return:
    """
    ret_dict = {}
    if len(kwargs["zjql_status_data"]) != 0:
        for i in kwargs["zjql_status_data"]:
            if i["comp_code"] in ret_dict:
                ret_dict[i["comp_code"]].append(i["ar_43"])
            else:
                ret_dict[i["comp_code"]] = [i["ar_43"]]
    return ret_dict


def ar_44_2(**kwargs):
    """
    整理上次资金青睐icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_44"]
    return ret_dict



ar_44_dict = {}
def ar_44(**kwargs):
    """
    资金青睐icon
    :param kwargs:
    :return:
    """
    global ar_44_dict
    ret_dict = {}
    ar_44_2_data = ar_44_2(piaochi_data=kwargs["piaochi_data"])
    ar_44_1_data = ar_44_1(zjql_status_data=kwargs["zjql_status_data"])
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])

    if ar_41_6_data:
        ar_44_dict = copy.deepcopy(ar_44_2_data)
        return ar_44_2_data

    for i in ar_43_dict:
        if ar_43_dict[i] == 0:
            if i in ar_44_2_data:
                zjql_icon_data = json.loads(ar_44_2_data[i])
                if zjql_icon_data is None or len(zjql_icon_data) == 0:
                    ret_dict[i] = json.dumps({"icon": '新上榜',
                                              'count': 1}, ensure_ascii=False)
                else:
                    if i in ar_44_1_data:
                        count_days = zjql_icon_data['count'] + 1
                        his_days = ar_44_1_data[i].count(0) + 1
                        if his_days >= 4 and his_days >= count_days + 2:
                            ret_dict[i] = json.dumps({"icon": f'10天{his_days}榜',
                                                      'count': count_days}, ensure_ascii=False)
                        elif count_days >= 2:
                            ret_dict[i] = json.dumps({"icon": f'连榜{count_days}天',
                                                      'count': count_days}, ensure_ascii=False)
                        else:
                            ret_dict[i] = json.dumps({"icon": '新上榜',
                                                      'count': 1}, ensure_ascii=False)
                    else:
                        ret_dict[i] = json.dumps({"icon": '新上榜',
                                                  'count': 1}, ensure_ascii=False)
            else:
                ret_dict[i] = json.dumps({"icon": '新上榜',
                                          'count': 1}, ensure_ascii=False)
        else:
            ret_dict[i] = json.dumps({})
    ar_44_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_45_1(**kwargs):
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_45"]
    return ret_dict


def ar_45(**kwargs):
    """
    资金青睐榜单中所有公司的北上资金总和
    :param kwargs:
    :return:
    """
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])
    ar_45_1_data = ar_45_1(piaochi_data=kwargs["piaochi_data"])

    if ar_41_6_data:
        return ar_45_1_data

    ret_dict = {}
    ar_41_4_data = ar_41_4(ef_list=kwargs["ef_list"])
    count_init = 0
    for i in ar_43_dict:
        if ar_43_dict[i] == 0:
            count_init += ar_41_4_data[i]['cd_22']

    for i in ar_43_dict:
        ret_dict[i] = count_init
    return ret_dict


def ar_46_1(**kwargs):
    ret_dict = {}
    for i in kwargs["piaochi_data"]:
        ret_dict[i["comp_code"]] = i["ar_46"]
    return ret_dict

def ar_46(**kwargs):
    """
    资金青睐新上榜公司数
    :param kwargs:
    :return:
    """
    ar_41_6_data = ar_41_6(ef_list=kwargs["ef_list"])
    ar_46_1_data = ar_46_1(piaochi_data=kwargs["piaochi_data"])

    if ar_41_6_data:
        return ar_46_1_data

    ret_dict = {}
    count_init = 0
    for i in ar_44_dict:
        data = json.loads(ar_44_dict[i])
        if len(data) == 0:
            continue
        else:
            if data['count'] == 1:
                count_init += 1

    for i in ar_44_dict:
        ret_dict[i] = count_init
    return ret_dict


def ar_54_1(**kwargs):
    """
    业绩改善榜单过滤
    :param kwargs:
    :return:
    """
    ret_list = ar_4_1(ef_list=kwargs["ef_list"])
    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        if i["cd_5"] == "是" or i["cd_44"] == "是":
            ret_list.append(comp_code)

    trade_day = kwargs["trade_day"]
    trade_day_datetime = datetime.datetime.strptime(trade_day, '%Y-%m-%d')

    datetime_1 = datetime.datetime.strptime(f"{trade_day[:4]}-01-01", '%Y-%m-%d')
    datetime_2 = datetime.datetime.strptime(f"{trade_day[:4]}-05-01", '%Y-%m-%d')
    datetime_3 = datetime.datetime.strptime(f"{trade_day[:4]}-09-01", '%Y-%m-%d')
    datetime_4 = datetime.datetime.strptime(f"{trade_day[:4]}-11-01", '%Y-%m-%d')



    if datetime_1 <= trade_day_datetime <= datetime_2:
        report_range = f"{int(trade_day[:4]) - 1}Q3"
    elif datetime_2 < trade_day_datetime <= datetime_3:
        report_range = f"{int(trade_day[:4]) - 1}Q4"
    elif datetime_3 < trade_day_datetime <= datetime_4:
        report_range = f"{int(trade_day[:4])}Q2"
    else:
        report_range = f"{int(trade_day[:4])}Q3"

    date_sql = f"select n_day from mc_data_analytic_result where report_range='{report_range}' order by n_day desc limit 1"
    date_str = db.query_db(date_sql)
    if len(date_str) == 0:
        date_day = None
    else:
        date_day = date_str[0][0]

    if date_day is not None:
        rating_sql = f"select comp_code from mc_data_analytic_result where report_range='{report_range}'" \
                     f" and n_day='{date_day}' and rating=4"
        rating_db = db.query_db(rating_sql)
        if len(rating_db) == 0:
            rating_db = None
    else:
        rating_db = None

    if rating_db is not None:
        for i in rating_db:
            ret_list.append(i[0])

    return ret_list


ar_54_dict = {}
def ar_54(**kwargs):
    """
    业绩改善榜单内容及icon
    :param kwargs:
    :return:
    """
    global ar_54_dict
    ar_54_1_data = ar_54_1(ef_list=kwargs["ef_list"], trade_day=kwargs["trade_day"])
    trade_day = kwargs["trade_day"]
    trade_day_datetime = datetime.datetime.strptime(trade_day, '%Y-%m-%d')
    ret_dict = {}
    for i in kwargs['ef_list']:
        comp_code = i['comp_code']
        if comp_code in ar_54_1_data:
            ret_dict[comp_code] = json.dumps({})
            continue

        if i['cd_37'] is not None and len(i['cd_37']) != 0:
            data_list = json.loads(i['cd_37'])
            report_cycle = data_list[0]
            if report_cycle[4:] == 'Q4':
                line_1 = f'{report_cycle[:4]}年快报'
            elif report_cycle[4:] == 'Q3':
                line_1 = f'{report_cycle[:4]}年Q3快报'
            elif report_cycle[4:] == 'Q2':
                line_1 = f'{report_cycle[:4]}年Q2快报'
            else:
                line_1 = f'{report_cycle[:4]}年Q1快报'
            line_2 = i['cd_35']
            line_5 = i['cd_40']
            line_7 = i['cd_33']
            line_8 = i["cd_52"]
            line_9 = i["cd_53"]
            kb_date = data_list[1]
            kb_date_datetime = datetime.datetime.strptime(kb_date, '%Y-%m-%d')
            days = (trade_day_datetime - kb_date_datetime).days
            if line_8 is not None and line_9 is not None:
                if days < 14 and ((i['cd_36'] is None) or (line_2 > 1.2 * i['cd_32'])) and i['cd_34'] > 50000000:
                    if i['cd_39'] < 0 and line_8 < 0:
                        ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                          'line_2': f'{round(line_2,1)}%',
                                                          'line_3': '利润增长',
                                                          'line_4': f'{kb_date_datetime.month}月{kb_date_datetime.day}日披露',
                                                          'line_5': f'{round(line_5,1)}%',
                                                          'line_6': '去年同期增长',
                                                          'line_7': f'{round(line_7,1)}%',
                                                          'line_8': '券商预期增长',
                                                          'line_9': '大幅扭亏',
                                                          'bk_1': kb_date,
                                                          'bk_2': i['cd_41']
                                                          },
                                                         ensure_ascii=False)
                    elif line_5 < -0.1 and line_9 < -0.1 and line_2 > 25:
                        ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                          'line_2': f'{round(line_2,1)}%',
                                                          'line_3': '利润增长',
                                                          'line_4': f'{kb_date_datetime.month}月{kb_date_datetime.day}日披露',
                                                          'line_5': f'{round(line_5,1)}%',
                                                          'line_6': '去年同期增长',
                                                          'line_7': f'{round(line_7,1)}%',
                                                          'line_8': '券商预期增长',
                                                          'line_9': '增速回正',
                                                          'bk_1': kb_date,
                                                          'bk_2': i['cd_41']
                                                          },
                                                         ensure_ascii=False)
                    elif 0 < line_5 < 0.2 and i['cd_41'] > 0.2 and line_2 > 30:
                        ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                          'line_2': f'{round(line_2,1)}%',
                                                          'line_3': '利润增长',
                                                          'line_4': f'{kb_date_datetime.month}月{kb_date_datetime.day}日披露',
                                                          'line_5': f'{round(line_5,1)}%',
                                                          'line_6': '去年同期增长',
                                                          'line_7': f'{round(line_7,1)}%',
                                                          'line_8': '券商预期增长',
                                                          'line_9': '快速增长',
                                                          'bk_1': kb_date,
                                                          'bk_2': i['cd_41']
                                                          },
                                                         ensure_ascii=False)
                    elif line_7 is not None and line_7 > 0 and line_2 > 1.3 * line_7 and (line_2 > line_7 + 10):
                        ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                          'line_2': f'{round(line_2,1)}%',
                                                          'line_3': '利润增长',
                                                          'line_4': f'{kb_date_datetime.month}月{kb_date_datetime.day}日披露',
                                                          'line_5': f'{round(line_5,1)}%',
                                                          'line_6': '去年同期增长',
                                                          'line_7': f'{round(line_7,1)}%',
                                                          'line_8': '券商预期增长',
                                                          'line_9': '远超预期',
                                                          'bk_1': kb_date,
                                                          'bk_2': i['cd_41']
                                                          },
                                                         ensure_ascii=False)
                    else:
                        ret_dict[comp_code] = json.dumps({})
                else:
                    ret_dict[comp_code] = json.dumps({})
            else:
                ret_dict[comp_code] = json.dumps({})
        else:
            ret_dict[comp_code] = json.dumps({})

        if len(ret_dict[comp_code]) == 0:
            if i['cd_36'] is None or len(i['cd_36']) == 0:
                ret_dict[comp_code] = json.dumps({}, ensure_ascii=False)
            else:
                data_list = json.loads(i['cd_36'])
                report_cycle = data_list[0]
                if report_cycle[4:] == 'Q4':
                    line_1 = f'{report_cycle[:4]}年预告'
                elif report_cycle[4:] == 'Q3':
                    line_1 = f'{report_cycle[:4]}年Q3预告'
                elif report_cycle[4:] == 'Q2':
                    line_1 = f'{report_cycle[:4]}年Q2预告'
                else:
                    line_1 = f'{report_cycle[:4]}年Q1预告'
                line_2 = i['cd_32']
                line_5 = i['cd_30']
                line_7 = i['cd_33']
                line_8 = i["cd_54"]
                line_9 = i["cd_55"]
                yg_date = data_list[1]
                yg_date_datetime = datetime.datetime.strptime(yg_date, '%Y-%m-%d')
                days = (trade_day_datetime - yg_date_datetime).days
                if line_8 is not None and line_9 is not None:
                    if days <= 14 and i["cd_31"] > 50000000:
                        if i["cd_29"] < 0 and line_8 < 0:
                            ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                              'line_2': f'{round(line_2,1)}%',
                                                              'line_3': '利润增长',
                                                              'line_4': f'{yg_date_datetime.month}月{yg_date_datetime.day}日披露',
                                                              'line_5': f'{round(line_5,1)}%',
                                                              'line_6': '去年同期增长',
                                                              'line_7': f'{round(line_7,1)}%',
                                                              'line_8': '券商预期增长',
                                                              'line_9': '大幅扭亏',
                                                              'bk_1': yg_date,
                                                              'bk_2': i['cd_38']
                                                              },
                                                     ensure_ascii=False)
                        elif line_5 < -0.1 and line_9 < -0.1 and line_2 > 25:
                            ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                      'line_2': f'{round(line_2,1)}%',
                                                      'line_3': '利润增长',
                                                      'line_4': f'{yg_date_datetime.month}月{yg_date_datetime.day}日披露',
                                                      'line_5': f'{round(line_5,1)}%',
                                                      'line_6': '去年同期增长',
                                                      'line_7': f'{round(line_7,1)}%',
                                                      'line_8': '券商预期增长',
                                                      'line_9': '增速回正',
                                                              'bk_1': yg_date,
                                                              'bk_2': i['cd_38']
                                                              },
                                                     ensure_ascii=False)
                        elif 0 < line_5 < 0.2 and i['cd_38'] > 0.2 and line_2 > 30:
                            ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                      'line_2': f'{round(line_2,1)}%',
                                                      'line_3': '利润增长',
                                                      'line_4': f'{yg_date_datetime.month}月{yg_date_datetime.day}日披露',
                                                      'line_5': f'{round(line_5,1)}%',
                                                      'line_6': '去年同期增长',
                                                      'line_7': f'{round(line_7,1)}%',
                                                      'line_8': '券商预期增长',
                                                      'line_9': '快速增长',
                                                              'bk_1': yg_date,
                                                              'bk_2': i['cd_38']
                                                              },
                                                     ensure_ascii=False)
                        elif line_7 is not None and line_7 > 0 and line_2 > 1.3*line_7 and (line_2 > line_7 + 10):
                            ret_dict[comp_code] = json.dumps({'line_1': line_1,
                                                      'line_2': f'{round(line_2,1)}%',
                                                      'line_3': '利润增长',
                                                      'line_4': f'{yg_date_datetime.month}月{yg_date_datetime.day}日披露',
                                                      'line_5': f'{round(line_5,1)}%',
                                                      'line_6': '去年同期增长',
                                                      'line_7': f'{round(line_7,1)}%',
                                                      'line_8': '券商预期增长',
                                                      'line_9': '远超预期',
                                                              'bk_1': yg_date,
                                                              'bk_2': i['cd_38']
                                                              },
                                                     ensure_ascii=False)
                        else:
                            ret_dict[comp_code] = json.dumps({})
                    else:
                        ret_dict[comp_code] = json.dumps({})
                else:
                    ret_dict[comp_code] = json.dumps({})

    ar_54_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_55(**kwargs):
    """
    业绩改善几家超预期的
    :param kwargs:
    :return:
    """
    ret_dict = {}
    count_js = 0
    for i in ar_54_dict:
        data = json.loads(ar_54_dict[i])
        if len(data) != 0 and (data['line_9'] == '大幅扭亏' or data['line_9'] == '增速回正'):
            count_js += 1

    for i in ar_54_dict:
        ret_dict[i] = count_js

    return ret_dict

ar_56_1_dict = {}
def ar_56_1(**kwargs):
    """
    整理高风险股所需指标
    :param kwargs:
    :return:
    """
    global ar_56_1_dict
    ret_dict = {}
    for i in kwargs['ef_list']:
        comp_code = i['comp_code']
        sql = "select yjzl_assess,ylnl_assess,xjl_assess,czyl_assess,qtfx_assess,ray_1,ray_5,ray_2,ray_3,ray_4 from mc_data_analytic_result where " \
              f"comp_code='{comp_code}' and report_range not like '%Q1%' ORDER BY report_range desc,n_day desc limit 1"
        ret_data = db.query_db_dict(sql)
        if len(ret_data) == 0:
            continue
        else:
            data = ret_data[0]
            ret_dict[comp_code] = data
    ar_56_1_dict = copy.deepcopy(ret_dict)
    return ret_dict


ar_56_dict = {}
def ar_56(**kwargs):
    """
    判断最新的数据是否为高风险股榜单
    gfx_status
    :param kwargs:
    :return:
    """
    global ar_56_dict
    ret_dict = {}
    ar_4_1_data = ar_4_1(ef_list=kwargs["ef_list"])
    ar_56_1_data = ar_56_1(ef_list=kwargs['ef_list'])
    for i in ar_56_1_data:
        if i in ar_4_1_data:
            ret_dict[i] = 1
        else:
            data_list = [ar_56_1_data[i]['yjzl_assess'], ar_56_1_data[i]['ylnl_assess'],
                         ar_56_1_data[i]['xjl_assess'], ar_56_1_data[i]['czyl_assess'], ar_56_1_data[i]['qtfx_assess']]
            jic_count = data_list.count(0)
            jiaocha_count = data_list.count(1)
            if jic_count >= 1 or jiaocha_count >= 3:
                ret_dict[i] = 0
            else:
                ret_dict[i] = 1
    ar_56_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_57(**kwargs):
    """
    高风险股等级及
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        comp_code = i['comp_code']
        ray_1_dict = json.loads(ar_56_1_dict[comp_code]['ray_1'])
        ray_5_dict = json.loads(ar_56_1_dict[comp_code]['ray_5'])
        if not isinstance(ray_1_dict, dict) or not isinstance(ray_5_dict, dict) or (len(ray_1_dict) == 0) or (ray_1_dict is None) or (len(ray_5_dict) == 0) or \
                (ray_5_dict is None):
            ret_dict[comp_code] = None
        else:

            data_list = [ar_56_1_dict[comp_code]['yjzl_assess'], ar_56_1_dict[comp_code]['ylnl_assess'],
                         ar_56_1_dict[comp_code]['xjl_assess'], ar_56_1_dict[comp_code]['czyl_assess'], ar_56_1_dict[comp_code]['qtfx_assess']]
            jic_count = data_list.count(0)
            jiaocha_count = data_list.count(1)
            if ray_1_dict['ray_1_1'] != '' or ray_5_dict['ray_5_0'] != '' or ray_5_dict['ray_5_1'] != '':
                ret_dict[comp_code] = '极高风险'
            elif jic_count >= 2:
                ret_dict[comp_code] = '极高风险'
            elif jic_count >= 1 and jiaocha_count >= 2:
                ret_dict[comp_code] = '极高风险'
            elif jiaocha_count >= 4:
                ret_dict[comp_code] = '极高风险'
            elif jic_count == 1 and jiaocha_count < 2:
                ret_dict[comp_code] = '高风险'
            elif jic_count == 0 and jiaocha_count == 3:
                ret_dict[comp_code] = '中高风险'
            else:
                ret_dict[comp_code] = '中高风险'
    return ret_dict


def ar_58(**kwargs):
    """
    高风险股icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        icon_list = []
        comp_code = i['comp_code']
        ray_1_dict = json.loads(ar_56_1_dict[comp_code]['ray_1'])
        ray_2_dict = json.loads(ar_56_1_dict[comp_code]['ray_2'])
        ray_3_dict = json.loads(ar_56_1_dict[comp_code]['ray_3'])
        ray_4_dict = json.loads(ar_56_1_dict[comp_code]['ray_4'])
        ray_5_dict = json.loads(ar_56_1_dict[comp_code]['ray_5'])
        if isinstance(ray_1_dict, dict) and isinstance(ray_2_dict, dict) and isinstance(ray_3_dict, dict) \
            and isinstance(ray_4_dict, dict) and isinstance(ray_5_dict, dict):
            if len(ray_1_dict) != 0 and len(ray_2_dict) != 0 and len(ray_3_dict) != 0 and len(ray_4_dict) != 0 and \
                    len(ray_5_dict) != 0:
                if ray_5_dict['ray_5_0'] != '':
                    icon_list.append('退市风险')
                if ray_5_dict['ray_5_1'] != '':
                    icon_list.append('财务造假')
                if ray_1_dict['ray_1_1'] != '':
                    icon_list.append('资不抵债')
                if ray_2_dict['ray_2_1'] != '':
                    icon_list.append('毛利亏损')
                elif ray_1_dict['ray_1_2'] != '':
                    icon_list.append('主业亏损')
                if ray_3_dict['ray_3_4'] != '':
                    icon_list.append('五年经营缺口')
                elif ray_3_dict['ray_3_2'] != '':
                    icon_list.append('经营缺口')
                if ray_4_dict['ray_4_1'] != '' or ray_4_dict['ray_4_2'] != '' or ray_4_dict['ray_4_4'] != '':
                    icon_list.append('付息困难')
                elif ray_4_dict['ray_4_5'] != '':
                    icon_list.append('还本困难')
                elif ray_4_dict['ray_4_6'] != '':
                    icon_list.append('高贷高质押')
                if ray_5_dict['ray_5_5'] != '':
                    icon_list.append('大额商誉')
                if ray_1_dict['ray_1_5'] != '':
                    icon_list.append('高非主业')
                if ray_5_dict['ray_5_4'] != '':
                    icon_list.append('大存大贷')
                if ray_5_dict['ray_5_6'] != '' and ray_5_dict['ray_5_6']['line_1'] == '存在大额存货跌价风险':
                    icon_list.append('存货跌价')
                if ray_5_dict['ray_5_7'] != '' and ray_5_dict['ray_5_7']['line_1'] == '存在大额坏账风险':
                    icon_list.append('大额坏账')
                if ray_1_dict['ray_1_7'] != '':
                    icon_list.append('变现能力大降')
                if ray_3_dict['ray_3_5'] != '' or ray_3_dict['ray_3_6'] != '':
                    icon_list.append('投资激进')
                if ray_5_dict['ray_5_8'] != '':
                    icon_list.append('质押爆仓')
                if ray_2_dict['ray_2_5'] != '':
                    icon_list.append('ROE大降')
                if ray_2_dict['ray_2_6'] != '':
                    icon_list.append('毛利率大降')
                if ray_5_dict['ray_5_3'] != '':
                    icon_list.append('收入挂帐')
                if ray_2_dict['ray_2_7'] != '':
                    icon_list.append('周转率大降')
                if ray_1_dict['ray_1_6'] != '':
                    icon_list.append('客户拖款')
                if ray_1_dict['ray_1_4'] != '':
                    icon_list.append('依赖并购')
                if ray_2_dict['ray_2_3'] != '':
                    icon_list.append('ROE极低')
                if ray_3_dict['ray_3_1'] != '':
                    icon_list.append('现金下滑')
                if ray_5_dict['ray_5_2'] != '':
                    icon_list.append('大客户依赖')

        ret_dict[comp_code] = json.dumps(icon_list, ensure_ascii=False)
    return ret_dict

ar_59_1_dict = {}
def ar_59_1(**kwargs):
    """
    获取上一次财报避雷数据
    :param kwargs:
    :return:
    """
    global ar_59_1_dict
    ret_dict = {}
    for i in kwargs['ef_list']:
        comp_code = i['comp_code']
        report_range_sql = f"select report_range from mc_data_analytic_result where comp_code='{comp_code}' " \
                           "group by report_range order by report_range desc limit 2"
        last_report_range = db.query_db(report_range_sql)
        if len(last_report_range) == 2:
            last_report_range = last_report_range[1][0]
            if last_report_range[4:] == "Q1":
                last_report_range = f"{int(last_report_range[:4]) -1}Q4"
        else:
            last_report_range = last_report_range[0][0]
            if last_report_range[4:] == "Q1":
                last_report_range = f"{int(last_report_range[:4]) -1}Q4"
        sql = "select yjzl_assess,ylnl_assess,xjl_assess,czyl_assess,qtfx_assess,ray_1,ray_5,ray_2,ray_3,ray_4 from mc_data_analytic_result where " \
              f"comp_code='{comp_code}' and report_range='{last_report_range}' ORDER BY n_day desc limit 1"
        ret_data = db.query_db_dict(sql)
        if len(ret_data) == 0:
            continue
        else:
            data = ret_data[0]
            ret_dict[comp_code] = data
    ar_59_1_dict = copy.deepcopy(ret_dict)
    return ret_dict


ar_59_2_dict = {}
def ar_59_2(**kwargs):
    """
    判断最新的数据是否为高风险股榜单
    gfx_status
    :param kwargs:
    :return:
    """
    global ar_59_2_dict
    ret_dict = {}
    ar_4_1_data = ar_4_1(ef_list=kwargs["ef_list"])
    ar_59_1_data = ar_59_1(ef_list=kwargs['ef_list'])
    for i in ar_59_1_data:
        if i in ar_4_1_data:
            ret_dict[i] = 1
        else:
            data_list = [ar_59_1_data[i]['yjzl_assess'], ar_59_1_data[i]['ylnl_assess'],
                         ar_59_1_data[i]['xjl_assess'], ar_59_1_data[i]['czyl_assess'], ar_59_1_data[i]['qtfx_assess']]
            jic_count = data_list.count(0)
            jiaocha_count = data_list.count(1)
            if jic_count >= 1 or jiaocha_count >= 3:
                ret_dict[i] = 0
            else:
                ret_dict[i] = 1
    ar_59_2_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_59(**kwargs):
    """
    高风险股封面数据
    :param kwargs:
    :return:
    """
    ret_dict = {}
    ar_4_1_data = ar_4_1_list
    ar_59_2_data = ar_59_2(ef_list=kwargs['ef_list'])

    add_conut = 0
    move_count = 0
    total_count = 0
    for i in ar_56_dict:
        if i in ar_4_1_data:
            continue
        if i in ar_59_2_data:
            if ar_56_dict[i] == 0 and ar_59_2_data[i] != 0:
                add_conut += 1
            elif ar_56_dict[i] != 0 and ar_59_2_data[i] == 0:
                move_count += 1

        else:
            if ar_56_dict[i] == 0:
                add_conut += 1

        if ar_56_dict[i] == 0:
            total_count += 1

    for i in ar_56_dict:
        ret_dict[i] = json.dumps({'add_comp': add_conut,
                                  'move_comp': move_count,
                                  'total_comp': total_count})

    return ret_dict


def ar_60(**kwargs):
    """
    查看是否处于业绩改善榜单
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_54_dict:
        data = json.loads(ar_54_dict[i])
        if len(data) == 0:
            ret_dict[i] = 1
        else:
            ret_dict[i] = 0
    return ret_dict


def ar_61(**kwargs):
    """
    用于业绩改善日期排序的
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_54_dict:
        data = json.loads(ar_54_dict[i])
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['bk_1']
    return ret_dict


def ar_62(**kwargs):
    """
    用于业绩改善日期排序的增速问题
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_54_dict:
        data = json.loads(ar_54_dict[i])
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['bk_2']
    return ret_dict


def ar_63(**kwargs):
    """
    业绩改善icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_54_dict:
        data = json.loads(ar_54_dict[i])
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['line_9']
    return ret_dict


def ar_64(**kwargs):
    """
    自选股资金信号icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_41_dict:
        try:
            data = json.loads(ar_41_dict[i])
        except TypeError:
            data = {}
        if len(data) == 0:
            ret_dict[i] = None
        elif data['backup'][0] == 0:
            ret_dict[i] = 0
        elif data['backup'][0] == 1:
            ret_dict[i] = 1
        elif data['backup'][0] == 2:
            ret_dict[i] = 2
        else:
            ret_dict[i] = None
    return ret_dict


def ar_65(**kwargs):
    """
    资金青睐类型
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_42_dict:
        try:
            data = json.loads(ar_42_dict[i])
        except TypeError:
            data = {}
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['type']
    return ret_dict


def ar_66(**kwargs):
    """
    资金青睐排序2
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_42_dict:
        try:
            data = json.loads(ar_42_dict[i])
        except TypeError:
            data = {}
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['scale']
    return ret_dict


def ar_67(**kwargs):
    """
    资金青睐查询icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in ar_44_dict:
        try:
            data = json.loads(ar_44_dict[i])
        except TypeError:
            data = {}
        if len(data) == 0:
            ret_dict[i] = None
        else:
            ret_dict[i] = data['icon']
    return ret_dict


def ar_69(**kwargs):
    """
     高风险股新增或者解除标示
     0标示新增，1标示解除
     :param kwargs:
     :return:
     """
    ret_dict = {}

    for i in ar_56_dict:
        if i in ar_59_2_dict:
            if ar_56_dict[i] == 0 and ar_59_2_dict[i] != 0:
                ret_dict[i] = 0
            elif ar_56_dict[i] != 0 and ar_59_2_dict[i] == 0:
                ret_dict[i] = 1
        else:
            if ar_56_dict[i] == 0:
                ret_dict[i] = 0

    return ret_dict


def ar_70(**kwargs):
    """
    自选股三日涨跌幅
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs['ef_list']:
        ret_dict[i['comp_code']] = i['cd_47']
    return ret_dict


def ar_71_1(**kwargs):
    """
    市盈率
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs['ef_list']:
        ret_dict[i['comp_code']] = i['cd_2']
    return ret_dict


def ar_71_2(**kwargs):
    """
    过去三年的市盈率平均值
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs['ef_list']:
        ret_dict[i['comp_code']] = i['cd_25']
    return ret_dict


def ar_71_3(**kwargs):
    """
    过去三年的市盈率标准差
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs['ef_list']:
        ret_dict[i['comp_code']] = i['cd_26']
    return ret_dict


def ar_71_4(**kwargs):
    """
    批量获取最新期的gzjwsp_icon
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        sql = f"select gzjwsp_icon,n_day from mc_data_analytic_result where comp_code='{comp_code}' " \
              f"ORDER BY report_range desc,n_day desc limit 1"
        ret_data = db.query_db(sql)
        if len(ret_data) == 0:
            ret_dict[comp_code] = {}
        else:
            ret_dict[comp_code] = json.loads(ret_data[0][0])
    return ret_dict


ar_71_dict = {}
def ar_71(**kwargs):
    """
    自选股估值信号
    0严重低估 1低估 2无
    :param kwargs:
    :return:
    """
    global ar_71_dict
    ret_dict = {}
    ar_71_1_data = ar_71_1(ef_list=kwargs['ef_list'])
    ar_71_2_data = ar_71_2(ef_list=kwargs['ef_list'])
    ar_71_3_data = ar_71_3(ef_list=kwargs['ef_list'])
    ar_71_4_data = ar_71_4(ef_list=kwargs['ef_list'])
    for i in ar_71_1_data:
        if ar_71_1_data[i] is None or ar_71_2_data[i] is None or ar_71_3_data[i] is None:
            ret_dict[i] = 2
        elif len(ar_71_4_data[i]) == 0 or ar_71_4_data[i]["icon"] == "估值无效" or ar_71_4_data[i]["icon"] == "":
            ret_dict[i] = 2
        else:
            if ar_71_1_data[i] < (ar_71_2_data[i] - ar_71_3_data[i]):
                ret_dict[i] = 0
            elif ar_71_1_data[i] < (ar_71_2_data[i] - 0.4*ar_71_3_data[i]):
                ret_dict[i] = 1
            else:
                ret_dict[i] = 2
    ar_71_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_80(**kwargs):
    """
    流通市值
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ltg_data"]:
        ret_dict[i["comp_code"]] = [i["ALIQMV"]]
    return ret_dict


def ar_1(**kwargs):
    """
    总市值
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        ret_dict[i["comp_code"]] = [i["cd_1"]]
    return ret_dict


def ar_81(**kwargs):
    """
    整理最近9天上榜数
    :param kwargs:
    :return:
    """
    ret_dict = {}
    if len(kwargs["zjql_status_data"]) != 0:
        for i in kwargs["zjql_status_data"]:
            if i["comp_code"] in ret_dict:
                ret_dict[i["comp_code"]].append(i["ar_43"])
            else:
                ret_dict[i["comp_code"]] = [i["ar_43"]]

    ret_dict_new = {}
    for i in ret_dict:
        his_days = ret_dict[i].count(0) + 1
        ret_dict_new[i] = his_days

    return ret_dict_new


def ar_82(**kwargs):
    """
    A股总公司数
    :param kwargs:
    :return:
    """
    ret_dict = {}
    filter_list = []
    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        if comp_code not in ar_4_1_list:
            filter_list.append(comp_code)

    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        ret_dict[comp_code] = len(filter_list)
    return ret_dict


def ar_83(**kwargs):
    """
    查询行业
    :param kwargs:
    :return:
    """
    ret_dict = {}
    filter_list = []
    for i in kwargs["ef_list"]:
        comp_code = i["comp_code"]
        if comp_code not in ar_4_1_list:
            filter_list.append(comp_code)

    filter_tup = tuple(filter_list)
    filter_tup_str = "','".join(filter_tup)
    sql = f"SELECT comp_code,ind_title from mc_data_indclass where comp_code in ('{filter_tup_str}')"
    ind_data = db.query_db_dict(sql)
    ind_comp = {}  # {'00002.SZ': "银行", "000004.SZ": "医疗"}
    for comp in ind_data:
        if comp["ind_title"] in ind_comp:
            ind_comp[comp["ind_title"]].append(comp["comp_code"])
        else:
            ind_comp[comp["ind_title"]] = [comp["comp_code"]]

    for i in ind_comp:
        for a in ind_comp[i]:
            ret_dict[a] = len(ind_comp[i])

      # 合并金融股
    for i in ar_4_1_list:
        ret_dict[i] = None

    return ret_dict


def ar_84(**kwargs):
    """
       判断当天是否为北向资金交易日
       :param kwargs:
       :return:
       """
    ret_dict = {}

    for i in kwargs["ef_list"]:
        ret_dict[i["comp_code"]] = 0

    return ret_dict


def ar_85_5(**kwargs):
    """
    判断当天是否为北向资金交易日
    :param kwargs:
    :return:
    """
    cd_56 = kwargs["ef_list"][0]['cd_56']
    if cd_56:
        return 1
    else:
        return 0


def ar_85_1(**kwargs):
    """
    获取当天的cd_21,cd_22,cd_18
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["ef_list"]:
        ret_dict[i["comp_code"]] = {"cd_21": i["cd_21"], "cd_22": i["cd_22"], "cd_18": i["cd_18"]}
    return ret_dict


def ar_85_2(**kwargs):
    """
    梳理最近20天北向资金交易日情况
    :param kwargs:
    :return:
    """
    ret_dict = {}
    arr_dict = {}
    day_10_sum_dict = {}
    day_20_sum_dict = {}

    day_10_gt_zeor_dict = {}
    day_20_gt_zeor_dict = {}

    last_day_je = {}

    for comp_code_info in kwargs["last_20_day_bxzj_dict"]:
        comp_code = comp_code_info["comp_code"]
        n_day = str(comp_code_info["n_day"])
        cd_22 = comp_code_info["cd_22"]
        if comp_code in arr_dict:
            arr_dict[comp_code][n_day] = cd_22
        else:
            arr_dict[comp_code] = {n_day: cd_22}

    for comp_code in arr_dict:
        comp_code_arr_dict = arr_dict[comp_code]
        sort_list = sorted(comp_code_arr_dict.items(), key=lambda item: item[0], reverse=True)
        if len(sort_list) > 10:
            day_10_list = sort_list[:10]
            day_10_sum = sum([i[1] for i in day_10_list])
            day_10_sum_dict[comp_code] = day_10_sum

            day_10_gt_zeor_days = [i[1] > 0 for i in day_10_list].count(True)
            day_10_gt_zeor_dict[comp_code] = day_10_gt_zeor_days

        else:
            day_10_sum = sum([i[1] for i in sort_list])
            day_10_sum_dict[comp_code] = day_10_sum

            day_10_gt_zeor_days = [i[1] > 0 for i in sort_list].count(True)
            day_10_gt_zeor_dict[comp_code] = day_10_gt_zeor_days

        day_20_sum = sum([i[1] for i in sort_list])
        day_20_sum_dict[comp_code] = day_20_sum

        day_20_gt_zeor_days = [i[1] > 0 for i in sort_list].count(True)
        day_20_gt_zeor_dict[comp_code] = day_20_gt_zeor_days

        last_day_je[comp_code] = sort_list[0][1]

    for comp_code in arr_dict:
        ret_dict[comp_code] = (last_day_je[comp_code], day_10_sum_dict[comp_code], day_20_sum_dict[comp_code],
                               day_10_gt_zeor_dict[comp_code], day_20_gt_zeor_dict[comp_code])

    return ret_dict


def ar_85_3(**kwargs):
    """
    上一资金青睐数据
    save_data = (N, BN)
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for comp_code_info in kwargs['piaochi_data']:
        comp_code = comp_code_info['comp_code']

        ret_dict[comp_code] = comp_code_info['ar_85']

    return ret_dict


def ar_85_4(je):
    """
    格式化金额
    :param je
    :return:
    """
    ret_je = round(je / 100000000, 2)
    return ret_je


ar_85_dict = {}

def ar_85(**kwargs):
    """
    改版资金青睐
    :param kwargs:
    :return:
    """
    global ar_85_dict
    ret_dict = {}
    ar_85_3_data = ar_85_3(piaochi_data=kwargs['piaochi_data'])
    ar_85_5_data = ar_85_5(ef_list=kwargs['ef_list'])
    if not ar_85_5_data:
        ar_85_dict = copy.deepcopy(ar_85_3_data)
        return ar_85_3_data

    ar_85_2_data = ar_85_2(last_20_day_bxzj_dict=kwargs['last_20_day_bxzj_dict'])

    for comp_code_info in kwargs["ef_list"]:
        comp_code = comp_code_info['comp_code']
        if comp_code in ar_85_3_data:
            if ar_85_3_data[comp_code] is not None:
                last_ar_85_data = json.loads(ar_85_3_data[comp_code])
            else:
                last_ar_85_data = {}
        else:
            last_ar_85_data = {}

        if comp_code in ar_85_2_data:
            comp_code_tup_data = ar_85_2_data[comp_code]
        else:
            comp_code_tup_data = (0, 0, 0, 0, 0)

        if comp_code_tup_data[3] >= 6 and comp_code_tup_data[1] >= 500000000:
            XB10 = ar_85_4(comp_code_tup_data[1])
        else:
            XB10 = 0

        if comp_code_tup_data[-1] >= 12 and comp_code_tup_data[2] >= 1000000000:
            XB20 = ar_85_4(comp_code_tup_data[2])
        else:
            XB20 = 0

        if last_ar_85_data and comp_code_tup_data[0] > 0:
            format_zr_je = ar_85_4(comp_code_tup_data[0])
            if last_ar_85_data['save_data'][0] + 1 >= 3 and last_ar_85_data['save_data'][1] \
                    + format_zr_je >= 2:
                XBN = round(last_ar_85_data['save_data'][1] + format_zr_je, 2)
                XN = last_ar_85_data['save_data'][0] + 1
            else:
                XBN = 0
                XN = 0
        else:
            XBN = 0
            XN = 0

        if comp_code_tup_data[0] >= 100000000:
            XB1 = ar_85_4(comp_code_tup_data[0])
        else:
            XB1 = 0

        if comp_code_tup_data[0] > 0:
            XXBN = comp_code_tup_data[0]
        else:
            XXBN = 0

        Z = max(XB10, XB20, XBN, XB1)

        if Z > 0:
            if Z > 10:
                icon = '北上大买'
            else:
                icon = '北上买入'
            data_1 = ar_85_4(comp_code_tup_data[0])
            if XBN:
                data_2 = XBN
                data_3 = f'连续{XN}日净买'
                N = XN
                BN = XBN
            elif last_ar_85_data:
                data_2 = '--'
                data_3 = ''
                if XXBN:
                    N = last_ar_85_data['save_data'][0] + 1
                    format_zr_je = ar_85_4(comp_code_tup_data[0])
                    BN = last_ar_85_data['save_data'][1] + format_zr_je
                else:
                    N = 0
                    BN = 0
            else:
                data_2 = '--'
                data_3 = ''
                if XXBN:
                    N = 1
                    BN = XB1
                else:
                    N = 0
                    BN = 0

            if Z == XB10:
                data_4 = XB10
                data_5 = '10日内净买入'
                data_6 = f'北上资金在10日内净买入了{XB10}亿元'

                ret_data = json.dumps({
                    'data_1': data_1,
                    'data_2': data_2,
                    'data_3': data_3,
                    'data_4': data_4,
                    'data_5': data_5,
                    'data_6': data_6,
                    'data_7': data_5,
                    'icon': icon,
                    'save_data': (N, BN)
                }, ensure_ascii=False)
            elif Z == XB20:
                data_4 = XB20
                data_5 = '20日内净买入'
                data_6 = f'北上资金在20日内净买入了{XB20}亿元'
                ret_data = json.dumps({
                    'data_1': data_1,
                    'data_2': data_2,
                    'data_3': data_3,
                    'data_4': data_4,
                    'data_5': data_5,
                    'data_6': data_6,
                    'data_7': data_5,
                    'icon': icon,
                    'save_data': (N, BN)
                }, ensure_ascii=False)
            elif Z == XBN:
                if max(XB10, XB20) == 0:
                    data_4 = ar_85_4(comp_code_tup_data[2])
                    data_5 = '20日内净买入'
                elif XB10 > XB20:
                    data_4 = XB10
                    data_5 = '10日内净买入'
                else:
                    data_4 = XB20
                    data_5 = '20日内净买入'
                data_6 = f'北上资金连续{XN}日净买入，合计金额{XBN}亿元'
                data_7 = f'连续{XN}日净买入'
                ret_data = json.dumps({
                    'data_1': data_1,
                    'data_2': data_2,
                    'data_3': data_3,
                    'data_4': data_4,
                    'data_5': data_5,
                    'data_6': data_6,
                    'data_7': data_7,
                    'icon': icon,
                    'save_data': (N, BN)
                }, ensure_ascii=False)
            else:
                if max(XB10, XB20) == 0:
                    data_4 = ar_85_4(comp_code_tup_data[2])
                    data_5 = '20日内净买入'
                elif XB10 > XB20:
                    data_4 = XB10
                    data_5 = '10日内净买入'
                else:
                    data_4 = XB20
                    data_5 = '20日内净买入'
                data_6 = f'北上资金昨日大额净买入，金额达{XB1}亿元'
                data_7 = f'昨日净买入'
                ret_data = json.dumps({
                    'data_1': data_1,
                    'data_2': data_2,
                    'data_3': data_3,
                    'data_4': data_4,
                    'data_5': data_5,
                    'data_6': data_6,
                    'data_7': data_7,
                    'icon': icon,
                    'save_data': (N, BN)
                }, ensure_ascii=False)
        elif last_ar_85_data:
            if XXBN:
                N = last_ar_85_data['save_data'][0] + 1
                format_zr_je = ar_85_4(comp_code_tup_data[0])
                BN = last_ar_85_data['save_data'][1] + format_zr_je
                ret_data = json.dumps({'save_data': (N, BN)}, ensure_ascii=False)
            else:
                N = 0
                BN = 0
                ret_data = json.dumps({'save_data': (N, BN)}, ensure_ascii=False)
        else:
            if XXBN:
                N = 1
                BN = XB1
                ret_data = json.dumps({'save_data': (N, BN)}, ensure_ascii=False)
            else:
                N = 0
                BN = 0
                ret_data = json.dumps({'save_data': (N, BN)}, ensure_ascii=False)

        ret_dict[comp_code] = ret_data
    ar_85_dict = copy.deepcopy(ret_dict)
    return ret_dict


def ar_86(**kwargs):
    """
    北上大买入选金额（排序使用）
    :param kwargs:
    :return:
    """
    ret_dict = {}
    ar_85_data = ar_85_dict
    for i in ar_85_data:
        try:
            i_data = json.loads(ar_85_data[i])
        except TypeError:
            i_data = {}
        if 'data_7' in i_data:
            if i_data['data_7'] == '昨日净买入':
                ret_dict[i] = i_data['data_1']
            elif '连续' in i_data['data_7']:
                ret_dict[i] = i_data['data_2']
            else:
                ret_dict[i] = i_data['data_4']
        else:
            ret_dict[i] = None

    return ret_dict


def ar_87(**kwargs):
    """
    个股解票资金话术
    :param kwargs:
    :return:
    """
    ret_dict = {}
    ar_85_data = ar_85_dict
    for i in ar_85_data:
        try:
            i_data = json.loads(ar_85_data[i])
        except TypeError:
            i_data = {}
        if 'data_6' in i_data:
            ret_dict[i] = json.dumps({'icon': i_data['icon'], 'text': i_data['data_6']}, ensure_ascii=False)
        else:
            ret_dict[i] = None

    return ret_dict


def ar_88(**kwargs):
    """
    资金信号
    0 北上大买，1 北上买入
    :param kwargs:
    :return:
    """
    ret_dict = {}
    ar_85_data = ar_85_dict
    for i in ar_85_data:
        try:
            i_data = json.loads(ar_85_data[i])
        except TypeError:
            i_data = {}
        if 'data_6' in i_data:
            icon = i_data['icon']
            if icon == "北上大买":
                ret_dict[i] = 0
            else:
                ret_dict[i] = 1
        else:
            ret_dict[i] = None

    return ret_dict


def ar_89_1(**kwargs):
    """
    卓越公司清单
    :param kwargs:
    :return:
    """
    ret_list = []
    for i in kwargs["pjzy_db_data"]:
        ret_list.append(i["comp_code"])
    return ret_list


def ar_89_2(**kwargs):
    """
    昨天ar_71
    :param kwargs:
    :return:
    """
    ret_dict = {}
    for i in kwargs["yesterday_ar_71_dict"]:
        comp_code = i["comp_code"]
        ret_dict[comp_code] = i["ar_71"]
    return ret_dict


def ar_89(**kwargs):
    """
    判断昨天是否为新进入严重低估的公司
    0 不是， 1是 2是且为卓越公司
    :param kwargs:
    :return:
    """
    ret_dict = {}
    ar_89_1_data = ar_89_1(pjzy_db_data=kwargs["pjzy_db_data"])
    ar_89_2_data = ar_89_2(yesterday_ar_71_dict=kwargs["yesterday_ar_71_dict"])

    for comp_code in ar_71_dict:
        comp_code_ar_71 = ar_71_dict[comp_code]
        if comp_code_ar_71 == 0:
            if comp_code in ar_89_2_data:
                yesterday_ar_71 = ar_89_2_data[comp_code]
                if yesterday_ar_71 == 0:
                    ret_dict[comp_code] = 0
                else:
                    if comp_code in ar_89_1_data:
                        ret_dict[comp_code] = 2
                    else:
                        ret_dict[comp_code] = 1
            else:
                ret_dict[comp_code] = 0
        else:
            ret_dict[comp_code] = 0

    return ret_dict


def ar_count(trade_day):
    """
    计算结果
    :return:
    """
    #过滤出要更新的公司
    report_cycle_sql = f"select comp_code from mc_data_compdict"
    comp_code_data = db.query_db(report_cycle_sql)
    comp_code_list = tuple(i[0] for i in comp_code_data)

    if comp_code_list:

        last_nine_trade_day_sql = f"select n_day from mc_data_listrank where n_day<'{trade_day}' GROUP BY n_day DESC LIMIT 9"
        last_nine_trade_day_data = db.query_db(last_nine_trade_day_sql)
        last_nine_trade_day_tup = tuple(str(i[0]) for i in last_nine_trade_day_data)
        print(last_nine_trade_day_tup)
        if len(last_nine_trade_day_tup) == 0:
            zjql_status_data = {}
            piaochi_data = {}
        elif len(last_nine_trade_day_tup) == 1:
            last_nine_trade_day_tup = last_nine_trade_day_tup[0]
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank where n_day='{last_nine_trade_day_tup}'"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank where n_day='{last_nine_trade_day_tup}'"
            piaochi_data = db.query_db_dict(piaochi_sql)
        else:
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank where n_day in {last_nine_trade_day_tup}"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_date = last_nine_trade_day_tup[0]
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank where n_day='{piaochi_date}'"
            piaochi_data = db.query_db_dict(piaochi_sql)

        # 获取当前日期的wd_5
        wd_5_sql = f"select wd_5,wd_1,comp_code from mc_data_windformula where n_day='{trade_day}'"
        wd_5_data = db.query_db_dict(wd_5_sql)

        # 获取当日的流通股
        ltg_sql = f"select comp_code,LIQSHARE,ALIQMV from mc_data_everydayargsvalues where trade_day='{trade_day}'"
        ltg_data = db.query_db_dict(ltg_sql)

        # 获取最近20天北上资金交易日的数据
        last_20_day_sql = f"select n_day from mc_data_everydayformula where cd_56=1 and n_day<='{trade_day}' GROUP BY n_day DESC LIMIT 20"
        last_20_day_data = db.query_db(last_20_day_sql)
        last_20_day_data_tup = tuple(str(i[0]) for i in last_20_day_data)
        last_20_day_data_tup_str = "','".join(last_20_day_data_tup)

        if len(last_20_day_data_tup) == 0:
            last_20_day_bxzj_dict = {}
        else:
            last_20_day_bxzj_sql = f"select comp_code,n_day,cd_22 from mc_data_everydayformula" \
                                   f" where n_day in ('{last_20_day_data_tup_str}')"
            last_20_day_bxzj_dict = db.query_db_dict(last_20_day_bxzj_sql)


        if len(comp_code_list) == 1:
            comp_code = comp_code_list[0]
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                                f"and comp_code='{comp_code}'"
        else:
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                            f"and comp_code in {comp_code_list}"
        ef_list = db.query_db_dict(every_formula_sql)


        #评级卓越且严重低估的公司
        trade_day_datetime = datetime.datetime.strptime(trade_day, "%Y-%m-%d")
        q4_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-05-01", "%Y-%m-%d")
        q2_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-09-01", "%Y-%m-%d")
        q3_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-11-01", "%Y-%m-%d")
        if trade_day_datetime > q4_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q3"
        elif q4_datetime <= trade_day_datetime < q2_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q4"
        elif q2_datetime <= trade_day_datetime < q3_datetime:
            report_range = f"{trade_day[:4]}Q2"
        else:
            report_range = f"{trade_day[:4]}Q3"

        pjzy_db_sql = f"select comp_code from mc_data_analytic_result where n_day='{trade_day}' " \
                      f"and report_range='{report_range}' and rating=0"
        pjzy_db_data = db.query_db_dict(pjzy_db_sql)

        yesterday_ar_71_date_sql = f"select n_day from mc_data_listrank where n_day<'{trade_day}' ORDER BY n_day desc LIMIT 1"
        yesterday_ar_71_date = db.query_db(yesterday_ar_71_date_sql)
        yesterday_ar_71_date_data = yesterday_ar_71_date[0][0]
        yesterday_ar_71_data_sql = f"select comp_code,ar_71 from mc_data_listrank where n_day='{yesterday_ar_71_date_data}'"
        yesterday_ar_71_dict = db.query_db_dict(yesterday_ar_71_data_sql)



        ret_data = []
        function_list = [ar_40, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61,
                         ar_62, ar_63, ar_64, ar_65, ar_66, ar_67, ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89]

        function_args_dict = {"ef_list": ef_list, "trade_day": trade_day,
                              "comp_code_list": comp_code_list, 'piaochi_data': piaochi_data, "wd_5_data": wd_5_data,
                              "ltg_data": ltg_data, "zjql_status_data": zjql_status_data,
                              'last_20_day_bxzj_dict': last_20_day_bxzj_dict, "pjzy_db_data": pjzy_db_data,
                              "yesterday_ar_71_dict": yesterday_ar_71_dict}

        for fun in function_list:
            start_time = time.time()
            ret_dict = fun(**function_args_dict)
            end_time = time.time()
            print(function_list.index(fun),"耗时", end_time - start_time)
            ret_data.append(ret_dict)

        data_dict = merge_dict_formula(ret_data)

        for i in data_dict:
            data_dict[i].insert(0, i)
            data_dict[i].insert(1, trade_day)

        ret_data_lists = [tuple(data_dict[i]) for i in data_dict]
        insert_sql = "insert into mc_data_listrank (comp_code, n_day, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  " \
                     "ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61, ar_62, ar_63, ar_64, ar_65, ar_66, ar_67," \
                     " ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89) values " \
                     "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        # insert_sql = "insert into mc_data_analytic_result (comp_code, n_day, report_range, ar_1, ar_2, ar_3, ar_4, ar_5, " \
        #                  "ar_6, ar_7, ar_8, ar_9, ar_10, ar_11, ar_12, ar_13, ar_14," \
        #              "ar_15, ar_16, ar_17, ar_18, ar_19, ar_20, ar_21, after_rank, comp_num, ind_rank, ind_num, " \
        #              "rank_change, ind_rank_change, rating) values " \
        #              "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        db.insert_db(insert_sql, ret_data_lists)
        print(trade_day, "计算完成")


def new_comp_ar_count(trade_day, new_comp_code_list):
    """
    计算结果
    :return:
    """
    #过滤出要更新的公司
    report_cycle_sql = f"select comp_code from mc_data_compdict"
    comp_code_data = db.query_db(report_cycle_sql)
    comp_code_list = tuple(i[0] for i in comp_code_data)
    if comp_code_list:

        last_nine_trade_day_sql = f"select n_day from mc_data_listrank where n_day<'{trade_day}' GROUP BY n_day DESC LIMIT 9"
        last_nine_trade_day_data = db.query_db(last_nine_trade_day_sql)
        last_nine_trade_day_tup = tuple(str(i[0]) for i in last_nine_trade_day_data)
        if len(last_nine_trade_day_tup) == 0:
            zjql_status_data = {}
            piaochi_data = {}
        elif len(last_nine_trade_day_tup) == 1:
            last_nine_trade_day_tup = last_nine_trade_day_tup[0]
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank where n_day='{last_nine_trade_day_tup}'"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank where n_day='{last_nine_trade_day_tup}'"
            piaochi_data = db.query_db_dict(piaochi_sql)
        else:
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank where n_day in {last_nine_trade_day_tup}"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_date = last_nine_trade_day_tup[0]
            print(piaochi_date)
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank where n_day='{piaochi_date}'"
            piaochi_data = db.query_db_dict(piaochi_sql)

        # 获取当前日期的wd_5
        wd_5_sql = f"select wd_5,wd_1,comp_code from mc_data_windformula where n_day='{trade_day}'"
        wd_5_data = db.query_db_dict(wd_5_sql)

        # 获取当日的流通股
        ltg_sql = f"select comp_code,LIQSHARE,ALIQMV from mc_data_everydayargsvalues where trade_day='{trade_day}'"
        ltg_data = db.query_db_dict(ltg_sql)

        # 获取最近20天北上资金交易日的数据
        last_20_day_sql = f"select n_day from mc_data_everydayformula where cd_56=1 and n_day<='{trade_day}' GROUP BY n_day DESC LIMIT 20"
        last_20_day_data = db.query_db(last_20_day_sql)
        last_20_day_data_tup = tuple(str(i[0]) for i in last_20_day_data)
        last_20_day_data_tup_str = "','".join(last_20_day_data_tup)

        if len(last_20_day_data_tup) == 0:
            last_20_day_bxzj_dict = {}
        else:
            last_20_day_bxzj_sql = f"select comp_code,n_day,cd_22 from mc_data_everydayformula" \
                                   f" where n_day in ('{last_20_day_data_tup_str}')"
            last_20_day_bxzj_dict = db.query_db_dict(last_20_day_bxzj_sql)





        if len(comp_code_list) == 1:
            comp_code = comp_code_list[0]
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                                f"and comp_code='{comp_code}'"
        else:
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                            f"and comp_code in {comp_code_list}"
        ef_list = db.query_db_dict(every_formula_sql)

        # 评级卓越且严重低估的公司
        trade_day_datetime = datetime.datetime.strptime(trade_day, "%Y-%m-%d")
        q4_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-05-01", "%Y-%m-%d")
        q2_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-09-01", "%Y-%m-%d")
        q3_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-11-01", "%Y-%m-%d")
        if trade_day_datetime > q4_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q3"
        elif q4_datetime <= trade_day_datetime < q2_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q4"
        elif q2_datetime <= trade_day_datetime < q3_datetime:
            report_range = f"{trade_day[:4]}Q2"
        else:
            report_range = f"{trade_day[:4]}Q3"

        pjzy_db_sql = f"select comp_code from mc_data_analytic_result where n_day='{trade_day}' " \
                      f"and report_range='{report_range}' and rating=0"
        pjzy_db_data = db.query_db_dict(pjzy_db_sql)

        yesterday_ar_71_date_sql = f"select n_day from mc_data_listrank where n_day<'{trade_day}' ORDER BY n_day desc LIMIT 1"
        yesterday_ar_71_date = db.query_db(yesterday_ar_71_date_sql)
        yesterday_ar_71_date_data = yesterday_ar_71_date[0][0]
        yesterday_ar_71_data_sql = f"select comp_code,ar_71 from mc_data_listrank where n_day='{yesterday_ar_71_date_data}'"
        yesterday_ar_71_dict = db.query_db_dict(yesterday_ar_71_data_sql)


        ret_data = []
        function_list = [ar_40, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61,
                         ar_62, ar_63, ar_64, ar_65, ar_66, ar_67, ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89]

        function_args_dict = {"ef_list": ef_list, "trade_day": trade_day,
                              "comp_code_list": comp_code_list, 'piaochi_data': piaochi_data, "wd_5_data": wd_5_data,
                              "ltg_data": ltg_data, "zjql_status_data": zjql_status_data,
                              'last_20_day_bxzj_dict': last_20_day_bxzj_dict, "pjzy_db_data": pjzy_db_data,
                              "yesterday_ar_71_dict": yesterday_ar_71_dict}

        for fun in function_list:
            start_time = time.time()
            ret_dict = fun(**function_args_dict)
            end_time = time.time()
            print(function_list.index(fun),"耗时", end_time - start_time)
            ret_data.append(ret_dict)

        data_dict = merge_dict_formula(ret_data)

        for i in data_dict:
            data_dict[i].insert(0, i)
            data_dict[i].insert(1, trade_day)

        ret_data_lists = [tuple(data_dict[i]) for i in data_dict if i in new_comp_code_list]
        insert_sql = "insert into mc_data_listrank (comp_code, n_day, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  " \
                     "ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61, ar_62, ar_63, ar_64, ar_65, ar_66, ar_67," \
                     " ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89) values " \
                     "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        # insert_sql = "insert into mc_data_analytic_result (comp_code, n_day, report_range, ar_1, ar_2, ar_3, ar_4, ar_5, " \
        #                  "ar_6, ar_7, ar_8, ar_9, ar_10, ar_11, ar_12, ar_13, ar_14," \
        #              "ar_15, ar_16, ar_17, ar_18, ar_19, ar_20, ar_21, after_rank, comp_num, ind_rank, ind_num, " \
        #              "rank_change, ind_rank_change, rating) values " \
        #              "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        db.insert_db(insert_sql, ret_data_lists)
        print(trade_day, "计算完成")


def ar_count_test(trade_day):
    """
    计算结果
    :return:
    """
    #过滤出要更新的公司
    report_cycle_sql = f"select comp_code from mc_data_compdict"
    comp_code_data = db.query_db(report_cycle_sql)
    comp_code_list = tuple(i[0] for i in comp_code_data)
    if comp_code_list:

        last_nine_trade_day_sql = f"select n_day from mc_data_listrank2 where n_day<'{trade_day}' GROUP BY n_day DESC LIMIT 9"
        last_nine_trade_day_data = db.query_db(last_nine_trade_day_sql)
        last_nine_trade_day_tup = tuple(str(i[0]) for i in last_nine_trade_day_data)
        print(last_nine_trade_day_tup)
        if len(last_nine_trade_day_tup) == 0:
            zjql_status_data = {}
            piaochi_data = {}
        elif len(last_nine_trade_day_tup) == 1:
            last_nine_trade_day_tup = last_nine_trade_day_tup[0]
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank2 where n_day='{last_nine_trade_day_tup}'"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank2 where n_day='{last_nine_trade_day_tup}'"
            piaochi_data = db.query_db_dict(piaochi_sql)
        else:
            zjql_status_sql = f"select comp_code,ar_43 from mc_data_listrank2 where n_day in {last_nine_trade_day_tup}"
            zjql_status_data = db.query_db_dict(zjql_status_sql)
            piaochi_date = last_nine_trade_day_tup[0]
            piaochi_sql = f"select comp_code,ar_41,ar_44,ar_56,ar_42,ar_43,ar_45,ar_46,ar_65,ar_66,ar_67,ar_85 from mc_data_listrank2 where n_day='{piaochi_date}'"
            piaochi_data = db.query_db_dict(piaochi_sql)

        # 获取当前日期的wd_5
        wd_5_sql = f"select wd_5,wd_1,comp_code from mc_data_windformula where n_day='{trade_day}'"
        wd_5_data = db.query_db_dict(wd_5_sql)

        # 获取当日的流通股
        ltg_sql = f"select comp_code,LIQSHARE,ALIQMV from mc_data_everydayargsvalues where trade_day='{trade_day}'"
        ltg_data = db.query_db_dict(ltg_sql)

        # 获取最近20天北上资金交易日的数据
        last_20_day_sql = f"select n_day from mc_data_everydayformula where cd_56=1 and n_day<='{trade_day}' GROUP BY n_day DESC LIMIT 20"
        last_20_day_data = db.query_db(last_20_day_sql)
        last_20_day_data_tup = tuple(str(i[0]) for i in last_20_day_data)
        last_20_day_data_tup_str = "','".join(last_20_day_data_tup)

        if len(last_20_day_data_tup) == 0:
            last_20_day_bxzj_dict = {}
        else:
            last_20_day_bxzj_sql = f"select comp_code,n_day,cd_22 from mc_data_everydayformula" \
                                   f" where n_day in ('{last_20_day_data_tup_str}')"
            last_20_day_bxzj_dict = db.query_db_dict(last_20_day_bxzj_sql)





        if len(comp_code_list) == 1:
            comp_code = comp_code_list[0]
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                                f"and comp_code='{comp_code}'"
        else:
            every_formula_sql = f"select * from mc_data_everydayformula where n_day='{trade_day}' " \
                            f"and comp_code in {comp_code_list}"
        ef_list = db.query_db_dict(every_formula_sql)

        # 评级卓越且严重低估的公司
        trade_day_datetime = datetime.datetime.strptime(trade_day, "%Y-%m-%d")
        q4_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-05-01", "%Y-%m-%d")
        q2_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-09-01", "%Y-%m-%d")
        q3_datetime = datetime.datetime.strptime(f"{trade_day[:4]}-11-01", "%Y-%m-%d")
        if trade_day_datetime > q4_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q3"
        elif q4_datetime <= trade_day_datetime < q2_datetime:
            report_range = f"{int(trade_day[:4]) - 1}Q4"
        elif q2_datetime <= trade_day_datetime < q3_datetime:
            report_range = f"{trade_day[:4]}Q2"
        else:
            report_range = f"{trade_day[:4]}Q3"

        pjzy_db_sql = f"select comp_code from mc_data_analytic_result where n_day='{trade_day}' " \
                      f"and report_range='{report_range}' and rating=0"
        pjzy_db_data = db.query_db_dict(pjzy_db_sql)

        yesterday_ar_71_date_sql = f"select n_day from mc_data_listrank where n_day<'{trade_day}' ORDER BY n_day desc LIMIT 1"
        yesterday_ar_71_date = db.query_db(yesterday_ar_71_date_sql)
        yesterday_ar_71_date_data = yesterday_ar_71_date[0][0]
        yesterday_ar_71_data_sql = f"select comp_code,ar_71 from mc_data_listrank where n_day='{yesterday_ar_71_date_data}'"
        yesterday_ar_71_dict = db.query_db_dict(yesterday_ar_71_data_sql)


        ret_data = []
        function_list = [ar_40, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61,
                         ar_62, ar_63, ar_64, ar_65, ar_66, ar_67, ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89]

        function_args_dict = {"ef_list": ef_list, "trade_day": trade_day,
                              "comp_code_list": comp_code_list, 'piaochi_data': piaochi_data, "wd_5_data": wd_5_data,
                              "ltg_data": ltg_data, "zjql_status_data": zjql_status_data,
                              'last_20_day_bxzj_dict': last_20_day_bxzj_dict, "pjzy_db_data": pjzy_db_data,
                              "yesterday_ar_71_dict": yesterday_ar_71_dict}

        for fun in function_list:
            start_time = time.time()
            ret_dict = fun(**function_args_dict)
            end_time = time.time()
            print(function_list.index(fun),"耗时", end_time - start_time)
            ret_data.append(ret_dict)

        data_dict = merge_dict_formula(ret_data)

        for i in data_dict:
            data_dict[i].insert(0, i)
            data_dict[i].insert(1, trade_day)

        ret_data_lists = [tuple(data_dict[i]) for i in data_dict]
        insert_sql = "insert into mc_data_listrank2 (comp_code, n_day, ar_41, ar_42, ar_43, ar_44, ar_45, ar_46,  " \
                     "ar_54, ar_55, ar_56, ar_57, ar_58, ar_59, ar_60, ar_61, ar_62, ar_63, ar_64, ar_65, ar_66, ar_67," \
                     " ar_69, ar_70, ar_71, ar_80, ar_1, ar_81, ar_82, ar_83, ar_84, ar_85, ar_86, ar_87, ar_88, ar_89) values " \
                     "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        # insert_sql = "insert into mc_data_analytic_result (comp_code, n_day, report_range, ar_1, ar_2, ar_3, ar_4, ar_5, " \
        #                  "ar_6, ar_7, ar_8, ar_9, ar_10, ar_11, ar_12, ar_13, ar_14," \
        #              "ar_15, ar_16, ar_17, ar_18, ar_19, ar_20, ar_21, after_rank, comp_num, ind_rank, ind_num, " \
        #              "rank_change, ind_rank_change, rating) values " \
        #              "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        db.insert_db(insert_sql, ret_data_lists)
        print(trade_day, "计算完成")


