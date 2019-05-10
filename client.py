"""
Jaye API client
<http://www.jaye.cloud>
"""

import requests
import pandas as pd
import logging
import datetime
import pytz
import os

from urllib.parse import urlencode


class Error(Exception):
    pass


class ClientError(Error):
    pass


class BaseClient(object):
    def __init__(self, client_id=None, client_secret=None,
                 timezone='Asia/Seoul',
                 base_url= 'http://www.jaye.cloud'):
        self._requests = []
        self.set_timezone(timezone=timezone)
        self.base_url = base_url
        self.access_token_url = self.base_url + '/oauth/token'
        self.authorize_url = self.base_url + '/oauth/authorize'
        self._access_token = os.environ['ACCESS_TOKEN']
        self._refresh_token = os.environ['REFRESH_TOKEN']

    # default time_zone = 'Asia/Seoul'
    def set_timezone(self, timezone):
        self.timezone = pytz.timezone(timezone)

    def set_auth(self, access_token, refresh_token):
        # _access_token and _refresh_token must be stored in session when this applied flask web app
        self._access_token = access_token
        self._refresh_token = refresh_token
        resp = {'access_token': self._access_token,
                'refresh_token': self._refresh_token}
        print(resp)
        # self.set_auto_refresh_token()

    def get_token(self):
        resp = {'access_token': self._access_token,
                'refresh_token': self._refresh_token}
        return resp

    def _get(self, path, callback=None, params=None):
        if params:
            path += '?' + urlencode(params)
        return self._request('GET',
                             path=path,
                             callback=callback)

    def _post(self, path, callback=None, params=None, json_data=None):
        params = params or {}
        if params:
            path += '?' + urlencode(params)
        body = json_data
        return self._request('POST',
                             path=path,
                             callback=callback,
                             body=body)

    def _request(self, method, path, callback=None, body=None):
        now = datetime.datetime.utcnow()
        period_start = now - datetime.timedelta(minutes=10)
        self._requests = [dt for dt in self._requests if dt >= period_start]
        self._requests.append(now)
        log.debug('%d requests in last %d seconds',
                  len(self._requests),
                  (now - self._requests[0]).seconds)
        self._requests.append(datetime.datetime.utcnow())

        token = self._access_token
        headers = {"Authorization": "Bearer " + token,
                   "Content-type": "application/json"}
        if method == 'GET':
            resp = requests.get(self.base_url + path, headers=headers)
        elif method == 'POST':
            resp = requests.post(self.base_url + path, headers=headers, json=body)

        # return resp
        if callback:
            return callback(self._process_response(resp))
        else:
            return self._process_response(resp)

    def _process_response(self, resp):
        log.debug('< %s %s', resp.status_code, "something")
        try:
            data = resp.json()
        except (ValueError, UnicodeError) as e:
            print(e)
            raise Error(
                'could not decode response json',
                resp.status_code) from e

        return data



### Public REST API methods ###
class Client(BaseClient):
    def __init__(self,
                 timezone='Asia/Seoul',
                 base_url='http://www.jaye.cloud'):

        super(Client, self). \
            __init__(timezone=timezone,
                     base_url=base_url)


    ##### ACCOUNT ###############
    def get_profile(self, callback=None):
        return self._get('/accounts/profile/', callback=callback)

    ##### AMS ###############
    # or renamed as financial assets, finantial balance
    def get_accounts(self, callback=None):
        data = self._get('/ams/account/', callback=callback)
        df = pd.DataFrame(data)
        return df

    def post_accounts(self, accounts, callback=None):
        accounts = accounts.to_dict(orient='records')
        return self._post('/ams/account/', json_data=accounts)

    def get_subaccounts(self, callback=None):
        data = self._get('/ams/subaccount/', callback=callback)
        df = pd.DataFrame(data)
        return df

    def post_subaccounts(self, sub_accounts, callback=None):
        sub_accounts = sub_accounts.to_dict(orient='records')
        return self._post('/ams/subaccount/', json_data=sub_accounts)

    def get_commodities(self, callback=None):
        data = self._get('/ams/commodity/', callback=callback)
        df = pd.DataFrame(data)
        return df

    def get_currency(self, callback=None):
        data = self._get('/ams/currency/', callback=callback)
        df = pd.DataFrame(data)
        return df

    def get_ledger(self, callback=None):
        data = self._get('/ams/ledger/', callback)
        return data

    def post_ledger(self, ledger, callback=None):
        return self._post('/ams/ledger/', json_data=ledger)

    ##### MIS ###############
    def get_stocks(self, callback=None):
        params = None
        data = self._get('/mis/stocks/', params=params, callback=callback)
        df = pd.DataFrame(data)
        return df

    def get_daily_chart(self, commodity, start_date, end_date, callback=None):
        params = {'commodity': commodity, 'start_date': start_date, 'end_date': end_date}
        data = self._get('/mis/daily-chart/', params=params, callback=callback)
        df = pd.DataFrame(data)
        return df

    ##### FAS ###############
    def get_risk_report(self, report_name, callback=None):
        return self._get('/risk/report/{}'.format(report_name))

    def get_pl(self, callback=None):
        data = self._get('/quant/pl')
        pl = data['pl']
        pl_total = data['pl_total']
        pl = pd.DataFrame(pl)
        pl_total = pd.DataFrame(pl_total)
        return pl, pl_total

    def do_backtest(self, conclusion, tx_cost_rate=0.003, callback=None):
        conclusion = conclusion.to_dict(orient='records')
        settings = {'conclusion': conclusion,
                    'tx_cost_rate': tx_cost_rate}
        data = self._post('/quant/back-test', json_data=settings)
        pl = data['pl']
        pl_total = data['pl_total']
        pl = pd.DataFrame(pl)
        pl_total = pd.DataFrame(pl_total)
        return pl, pl_total

    ##### RAS ###############

    ##### UANDUS ###############
    def get_uandus_stocks(self, callback=None):
        params = None
        data = self._get('/uandus/stocks/', params=params, callback=callback)
        df = pd.DataFrame(data)
        return df

    def get_uandus_daily_chart(self, commodity, start_date, end_date, callback=None):
        params = {'commodity': commodity, 'start_date': start_date, 'end_date': end_date}
        data = self._get('/uandus/daily-chart/', params=params, callback=callback)
        df = pd.DataFrame(data)
        return df


