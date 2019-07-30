import requests, json
import logging
import datetime
import copy
import pymssql
import pandas as pd
from requests.auth import HTTPBasicAuth
from dateutil.relativedelta import relativedelta


class FieldConverter:
    """FieldConverter class"""
    def __init__(self):
        self.logger = logging.getLogger('global_optimization.util.report_util.FieldConverter')
        pass

    def parse_field_for_sending_to_core(self, field, publisher):
        """parse field for sending to API"""
        replacements = self._replacements()

        if publisher in replacements:
            for replacement in replacements[publisher]:
                if field == replacement["name_in_args"]:
                    return replacement["name_in_koddi_core"]

        return field

    def parse_field_for_returning_to_caller(self, field, publisher):
        """parse field for returning to caller"""
        replacements = self._replacements()

        if publisher in replacements:
            for replacement in replacements[publisher]:
                if field == replacement["name_in_koddi_core"]:
                    return replacement["name_in_args"]

        return field

    def _replacements(self):
        return {
            "hpa": [
                {
                    "name_in_args": "placement",
                    "name_in_koddi_core": "search_type",
                },
                {
                    "name_in_args": "rank_avg",
                    "name_in_koddi_core": "true_rank_avg",
                },
            ],
            "trivago": [
                {
                    "name_in_args": "rank_avg",
                    "name_in_koddi_core": "pos_avg",
                },
            ],
        }

class Report:
    """Report object"""
    def __init__(self, fields, dimensions, filters, start_date, end_date, site_id, publisher):
        self.fieldconverter = FieldConverter()

        self.publisher = publisher
        self._set_fields(fields)
        self._set_dimensions(dimensions)
        self._set_filters(filters)
        self.start_date = start_date
        self.end_date = end_date
        self.site_id = site_id

        self.logger = logging.getLogger('global_optimization.util.report_util.Report')

    def __str__(self):
        return str(self.serialize())

    def serialize(self):
        return {
            "fields": self.fields,
            "dimensions": self.dimensions,
            "filters": self.filters,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

    def _set_fields(self, fields):
        fields[:] = [self.fieldconverter.parse_field_for_sending_to_core(field, self.publisher) for field in fields]
        self.fields = fields

    def _set_dimensions(self, dimensions):
        dimensions[:] = [self.fieldconverter.parse_field_for_sending_to_core(field, self.publisher) for field in dimensions]
        self.dimensions = dimensions

    def _set_filters(self, filters):
        for i, f in enumerate(filters):
            filters[i]["field"] = self.fieldconverter.parse_field_for_sending_to_core(filters[i]["field"], self.publisher)

        self.filters = filters

class ReportingUtil:
    """Utility module for connecting to the Koddi Enterprise Reporting API (core)"""
    def __init__(self, params):
        self._validate_params(params)
        self.params = params
        self.fieldconverter = FieldConverter()
        self.logger = logging.getLogger('global_optimization.util.report_util.ReportingUtil')

    def get_data(self, report: Report):
        res = self._do_request(report)
        self.logger.debug(res)
        return self._response_to_array_dict(res)

    def _validate_params(self, params):
        required_keys = [
            'host',
            'user',
            'token',
        ]
        for key in required_keys:
            if not key in params:
                raise KeyError("Constructor params dictionary is missing required key "+key)

    def _do_request(self, report: Report):
        self.logger.debug('Making request for {}'.format(report))
        r = requests.post(
            self.params["host"]+"/api/reporting/"+str(report.site_id)+"/"+report.publisher,
            json=report.serialize(),
            auth=HTTPBasicAuth(self.params["user"], self.params["token"])
        )
        self.logger.debug("Got response of: {}".format(r))
        if r.ok:
            self.logger.debug(r)
        elif r.status_code == 400:
            self.logger.info("Failed for request. Status {}. Reason {}. Call: {} | {}".format(
                str(r.status_code),
                r.reason + ' ' + r.text,
                self.params["host"] + "/api/reporting/" + str(report.site_id) + "/" + report.publisher,
                report.serialize()))
        else:
            self.logger.error("Failed for request. Status {}. Reason {}. Call: {} | {}".format(
                                                                str(r.status_code),
                                                                r.reason + ' ' + r.text,
                                                                self.params["host"]+"/api/reporting/"+str(report.site_id)+"/"+report.publisher,
                                                               report.serialize()))


        return self._parse_response(r, report)

    def _parse_response(self, response, report: Report):
        if response.ok:
            try:
                decoded = response.json()
                self.logger.debug("Got parsed response of: {}".format(str(decoded)[0:3000]))
                if not "headers" in decoded:
                    return False

                for i, h in enumerate(decoded["headers"]):
                    decoded["headers"][i] = self.fieldconverter.parse_field_for_returning_to_caller(h, report.publisher)

                keys = decoded["pretty_headers"].keys()

                for key in keys:
                    decoded["pretty_headers"][self.fieldconverter.parse_field_for_returning_to_caller(key, report.publisher)] = decoded["pretty_headers"].pop(key)

                return decoded
            except:
                self.logger.error(f"Failed to parse response of {response}")
        else:
            return False

    def _response_to_array_dict(self, response):
        result = []
        if response == False:
            return False
        for row in response["data"]:
            newRow = {}
            for i in range(0, len(response["headers"])):
                newRow[response["headers"][i]] = row[i]

            result.append(newRow)

        return result

class ReportingUtilBatched(ReportingUtil):


    def _batch_report(self, report: Report, previous_report: Report=None, records_in_last_report=0):

        dt1_str = report.start_date
        dt2_str = report.end_date

        dt1 = datetime.datetime.strptime(dt1_str, '%Y-%m-%d')
        dt2 = datetime.datetime.strptime(dt2_str, '%Y-%m-%d')

        start_date = dt1
        end_date = dt1

        target_records = 100000

        if previous_report is not None:
            if previous_report.end_date >= report.end_date:
                return None
            else:
                start_date_prev_report = datetime.datetime.strptime(previous_report.start_date, '%Y-%m-%d')
                end_date_prev_report = datetime.datetime.strptime(previous_report.end_date, '%Y-%m-%d')
                days_prev_report = (end_date_prev_report - start_date_prev_report).days
                start_date = end_date_prev_report + relativedelta(days=1)

                if records_in_last_report > 0:
                    new_days = (1+days_prev_report) * target_records / records_in_last_report
                    new_days = min(new_days, 30)
                    end_date = min(dt2, start_date + relativedelta(days=new_days))
                else:
                    end_date = min(dt2, start_date + relativedelta(days=3))

        new_report = copy.deepcopy(report)
        new_report.start_date = start_date.strftime("%Y-%m-%d")
        new_report.end_date = end_date.strftime("%Y-%m-%d")

        return new_report


    def _do_request(self, report: Report):
        self.logger.info(f'Preparing batched request site {report.site_id} publisher {report.publisher} for {report}')
        batched_report = self._batch_report(report)
        response = []
        while batched_report is not None:
            batched_response = super()._do_request(batched_report)
            if batched_response is not False:
                batched_response_array = super()._response_to_array_dict(super()._do_request(batched_report))
                response_length = len(batched_response_array)
                self.logger.info(f'Report {batched_report} has {response_length} records')
                response.extend(batched_response_array)
                self.logger.debug(f'Added {response_length} to result. Results now has {len(response)} records')

            else:
                self.logger.info(f'Site {report.site_id} publisher {report.publisher} Report {batched_report} has no response')
                response_length = 0

            batched_report = self._batch_report(report, batched_report, response_length)

        return response

    def get_data(self, report: Report):
        res = self._do_request(report)
        res = False if len(res) == 0 else res
        return res

class APIParameter:

    def __init__(self, email, password, clientId, apiServer):
        self.email = email
        self.password = password
        self.clientId = clientId
        self.apiServer = apiServer


class APICall:

    def __init__(self, api_parameter: APIParameter):

        self.__email = api_parameter.email
        self.__password = api_parameter.password
        self.__clientId = api_parameter.clientId
        self.__apiServer = api_parameter.apiServer
        self.__token = None

        self.__set_token()
        self.__setHeaders()

    def __set_token(self):
        response = requests.request('POST',
                                    self.__apiServer + 'sessions/token',
                                    data=json.dumps({"email": self.__email, "password": self.__password, "clientId": self.__clientId}),
                                    headers={'Content-Type': "application/json", 'Accept': 'application/json'})

        try:
            self.__token = json.loads(response.text)['token']
            logging.debug("Got token: {}".format(self.__token))
        except Exception as error:
            logging.error('Failed to get auth token {}'.format(response.text))
            raise Exception("Could not get token")

    def __setHeaders(self):
        self.__headers = {
            'Content-Type': "application/json",
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.__token)
        }

    def makeRequest(self, endpoint, payload='', requestType="GET"):

        url = '{0}{1}'.format(self.__apiServer, endpoint)

        try:
            if (payload != ''):
                payload = json.dumps(payload)
                response = requests.request(requestType, url, data=payload, headers=self.__headers)
            else:
                response = requests.request(requestType, url, headers=self.__headers)

            message = "Making {} request with {} {} {}. Response {}. Reason {}". \
                        format(requestType, url, self.__headers, payload, response.status_code, response.text)

            if response.ok:
                logging.debug(message)
            else:
                print(message)
                raise Exception(message)

        except requests.exceptions.RequestException as e:
            logging.error(e)

        return response.json()


class Database:

    def __init__(self, db_parameter):

        self.db_parameter = db_parameter
        self.cnxn = None

        self.__set_connection()

    def __set_connection(self):

        logging.debug('Creating database connection. {}, {}, {}, {}'.format(self.db_parameter.url, self.db_parameter.username, self.db_parameter.password, self.db_parameter.database))
        self.cnxn = pymssql.connect(server = self.db_parameter.url,
                                   user = self.db_parameter.username,
                                   password = self.db_parameter.password,
                                   database = self.db_parameter.database,
                                   appname="tads_balance_redistributor",
                                   autocommit=True)

    def select_into_df(self, query):
        return pd.read_sql(query, self.cnxn)

    def select_into_dict(self, query):
        return self.select_into_df(query).to_dict(orient='records')


class DatabaseParameter:

    def __init__(self, url, username, password, database):
        self.url = url
        self.username = username
        self.password = password
        self.database = database