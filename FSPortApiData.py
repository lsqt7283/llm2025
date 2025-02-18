# -*- coding: utf-8 -*-
"""
Created on Fri Feb 7 15:13:47 2025

Run Data Query via Faceset Earnings Reports API (Chris Dietz)


@author: TQiu
"""


import datetime
import pandas as pd
import requests


def _get_json_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025"):
    """returns a json from api"""
    _api = "http://ace/dev/arc/fsenrich/api/getEnrichedPortfolioSecurities?"
    _api += "entityId="
    _api += entity_id
    _api += "&startDate="
    _api += start_date
    _api += "&endDate="
    _api += end_date
    _api += "&addSavedBatchScores=Y"
    return requests.get(_api).json()


def _parse_json_reports(reports_json):
    """returns a df of securities, a df of reports, a df of parents, a df of batch summary"""
    _summary = reports_json["batchHistogram"]
    _parents = reports_json["parentEntities"]
    _sec_data = []
    _reports = []
    for _sec in reports_json["securities"]:
        
        if len(_sec["issuerReports"]) == 0:
            pass #could process bad reports here
        else:
            _sid = _sec['securityId']
            _tmp = {}
            for _key in _sec.keys():
                if _key == "issuerReports":
                    _rpts = _sec[_key]
                elif _key == "fsQtrFundamentals":
                    pass #currently empty list, revisit later
                else:
                    _tmp.update({_key : _sec[_key]})
            _sec_data.append(_tmp)
            for _rpt in _rpts:
                if _rpt["batchAnalysis"] is None:
                    pass #could process bad reports here
                else:
                    _tmp = {}
                    _tmp.update({"securityId" : _sid})
                    for _key in _rpt.keys():
                        if _key == "batchAnalysis":
                            for _scr in _rpt[_key].keys():
                                _tmp.update({_scr : _rpt[_key][_scr]["score"], 
                                            _scr+"_comment" : _rpt[_key][_scr]["comment"]})
                        else:
                            _tmp.update({_key : _rpt[_key]})
                    _reports.append(_tmp)
    return pd.DataFrame(_sec_data), pd.DataFrame(_reports), pd.DataFrame(_parents), pd.DataFrame([_summary])


def get_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025", folder_path="."):
    print("...downloading earnings report......")
    df_sec, df_rpt, df_prt, df_bch = _parse_json_reports(_get_json_reports(entity_id, start_date, end_date))
    str_start = datetime.datetime.strptime(start_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_end = datetime.datetime.strptime(end_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_fname = folder_path + "/security_data_" + entity_id + str_start + str_end + ".csv"
    df_sec.to_csv(str_fname)
    str_fname = folder_path + "/reports_data_" + entity_id + str_start + str_end + ".csv"
    df_rpt.to_csv(str_fname)
    str_fname = folder_path + "/parents_data_" + entity_id + str_start + str_end + ".csv"
    df_prt.to_csv(str_fname)
    str_fname = folder_path + "/batch_data_" + entity_id + str_start + str_end + ".csv"
    df_bch.to_csv(str_fname)
    return


def load_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025", folder_path="."):
    str_start = datetime.datetime.strptime(start_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_end = datetime.datetime.strptime(end_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_fname = folder_path + "/security_data_" + entity_id + str_start + str_end + ".csv"
    df_sec = pd.read_csv(str_fname, index_col=0)
    str_fname = folder_path + "/reports_data_" + entity_id + str_start + str_end + ".csv"
    df_rpt = pd.read_csv(str_fname, index_col=0)
    print("loaded all securities and their earnings reports.")
    return df_sec, df_rpt
    

__all__ = ["get_reports", "load_reports"]