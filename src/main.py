from src.util import ReportingUtil, Report
from src.util import Database, DatabaseParameter
import pandas as pd
import numpy as np
import datetime
import tempfile
import boto3
import json
import logging

logging.getLogger('').handlers = []
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)


class Comparer:

    def __init__(self, site_id, site_company_group_id, start_date, end_date, enterprise_user, enterprise_token,
                enterprise_host, db_username, db_password, db_url, db_database, sns_success_arn, sns_failure_arn,
                 metrics
                ):

        self.site_id = site_id
        self.site_company_group_id = site_company_group_id
        self.start_date = start_date
        self.end_date = end_date
        self.enterprise_user = enterprise_user
        self.enterprise_token = enterprise_token
        self.enterprise_host = enterprise_host
        self.db_username = db_username
        self.db_password = db_password
        self.db_url = db_url
        self.db_database = db_database
        self.sns_success_arn = sns_success_arn
        self.sns_failure_arn = sns_failure_arn
        self.metrics = metrics
        self.s3_location = None

        logging.info(f'Created comparer {self}')

    def run(self, s3_bucket, s3_base_path):
        logging.info(f'Running comparer')
        self.compare()
        logging.info(f'Comparison complete')
        self.save_to_s3(s3_bucket, s3_base_path)
        logging.info(f'Posted results to {self.s3_location}')
        self.post_message()
        logging.info(f'Posted sns message')

    def __to_dict(self):
        return {
            'site_id': self.site_id,
            'site_company_group_id': self.site_company_group_id,
            'start_date_compare': self.start_date,
            'end_date_compare': self.end_date
        }

    def __get_enterprise_data(self):
        publisher = 'other_publishers'
        fields = ['transactions', 'profit', 'custom_decimal_4', 'custom_integer_3']
        dimensions = ['sub_placement', 'placement', 'report_date', 'hotel_id', 'hotel_chain']
        filters = [{'field': 'hotel_country', 'operation': 'in', 'value': 'US,CA', 'or_group': 'AND'},
                   {'field': 'channel', 'operation': '=', 'value': 'Advance', 'or_group': 'AND'},
                   {'field': 'placement', 'operation': '!=', 'value': 'SEO', 'or_group': 'AND'}]

        report = Report(
                fields, dimensions, filters, self.start_date, self.end_date, self.site_id, publisher)

        res = ReportingUtil({
                    "user": self.enterprise_user,
                    "token": self.enterprise_token,
                    "host": self.enterprise_host
                }).get_data(report)

        df_enterprise = pd.DataFrame(res)
        df_enterprise = df_enterprise.rename(index=str, columns={'profit': 'revenue',
                                                                 'transactions': 'bookings',
                                                                 'custom_decimal_4': 'consumedRevenue',
                                                                 'custom_integer_3': 'roomNights',
                                                                 'sub_placement': 'subChannelName',
                                                                 'placement': 'channelName',
                                                                 'report_date': 'date',
                                                                 'hotel_id': 'hotelCode',
                                                                 'hotel_chain': 'brand'})

        df_enterprise['channelName'] = df_enterprise['channelName'].str.upper()
        df_enterprise['subChannelName'] = df_enterprise['subChannelName'].str.upper()
        df_enterprise['hotelCode'] = df_enterprise['hotelCode'].str.replace('-', '')
        df_enterprise = df_enterprise[df_enterprise['channelName'] != 'SEO']
        df_enterprise['subChannelName'] = df_enterprise['subChannelName'].str.replace('BRAND + PROP', 'BRAND+PROP')
        df_enterprise['subChannelName'] = df_enterprise['subChannelName'].str.replace('COREBRAND', 'CORE BRAND')

        for c in ['date', 'hotelCode', 'brand', 'channelName', 'subChannelName']:
            df_enterprise[c].fillna(value='', inplace=True)

        for m in self.metrics:
            df_enterprise[m] = pd.to_numeric(df_enterprise[m], errors='coerce')

        df_enterprise = df_enterprise.groupby(['date', 'hotelCode', 'brand', 'channelName', 'subChannelName']).sum().reset_index()

        return df_enterprise


    def __get_pm_data(self):
        db_parameter = DatabaseParameter(self.db_url, self.db_username, self.db_password, self.db_database)
        db = Database(db_parameter)

        with open('queries/prodreporting.sql') as f:
            query = f.read()

        query = query.format(f"'{self.start_date}'", f"'{self.end_date}'", self.site_company_group_id)
        df = db.select_into_df(query)

        df['channelName'] = df['channelName'].str.upper()
        df['subChannelName'] = df['subChannelName'].str.upper()
        df['subChannelName'] = df['subChannelName'].str.replace('BRAND + PROP', 'BRAND+PROP')
        df['subChannelName'] = df['subChannelName'].str.replace('COREBRAND', 'CORE BRAND')
        df['hotelCode'] = df['hotelCode'].str.replace('-', '')

        return df


    def compare(self):
        enterprise = self.__get_enterprise_data()
        pm = self.__get_pm_data()

        compare = enterprise.merge(pm,
                                   how='outer',
                                   on=['hotelCode', 'date', 'channelName', 'subChannelName'],
                                   suffixes=('_enterprise', '_pm'))


        compare['hasDiff'] = 0
        for m in self.metrics:
            compare[f'{m}_enterprise'] = compare[f'{m}_enterprise'].fillna(0)
            compare[f'{m}_pm'] = compare[f'{m}_pm'].fillna(0)
            compare[f'{m}_diff'] = compare[f'{m}_enterprise'] - compare[f'{m}_pm']
            compare['hasDiff'] = np.where(compare[f'{m}_diff'].abs() < 0.01, compare['hasDiff'], 1)


        cols = ['hotelCode', 'date', 'channelName', 'subChannelName', 'brand_enterprise', 'bookings_enterprise',
               'roomNights_enterprise', 'revenue_enterprise', 'brand_pm', 'bookings_pm',
               'roomNights_pm', 'revenue_pm', 'bookings_diff', 'roomNights_diff', 'revenue_diff', 'hasDiff']

        compare = compare[cols]

        self.comparison = compare
        self.diffs = compare[compare['hasDiff']==1]

    def save_to_s3(self, s3_bucket, s3_base_path, withHeader=True):
        s3 = boto3.client('s3')

        currentStrTimestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        tempfileName = tempfile.NamedTemporaryFile(suffix='csv').name

        logging.info(f'Saved tempfile to {tempfileName}')
        self.comparison.to_csv(tempfileName, index=False, encoding="utf-8", header=withHeader)

        s3_path = s3_base_path.format(currentStrTimestamp)
        s3.upload_file(tempfileName, s3_bucket, s3_path)
        self.s3_location = f's3://{s3_bucket}/{s3_path}'

    def is_failure(self):
        num_diffs = len(self.diffs.index)
        if num_diffs > 0:
            return True
        else:
            return False

    def __create_message(self):

        msg = {}
        msg['executetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg['run_parameters'] = self.__to_dict()


        results = {}
        results['num_records'] = len(self.comparison.index)
        results['num_mismatches'] = len(self.diffs.index)
        results['is_full_match'] = not self.is_failure()
        results['s3_path'] = self.s3_location

        msg['results'] = results

        if self.is_failure():
            failure_str = 'Failure'
        else:
            failure_str = 'Success'

        msg['status'] = failure_str

        subject = f'Hilton Advance Dashboard Comparison: {failure_str}'

        msg_str = json.dumps(msg, indent=2)
        return msg_str, subject


    def post_message(self):
        sns = boto3.client('sns')

        msg, subject = self.__create_message()

        if self.is_failure():
            topic = self.sns_failure_arn
        else:
            topic = self.sns_success_arn

        sns.publish(TopicArn=topic,
                     Message=msg,
                     Subject=subject)

        logging.info(f'Posted message to {topic}. {msg}')


def do(db_password, enterprise_token):

    site_id = 136
    site_company_group_id = 28
    start_date = '2019-06-01'
    end_date = '2019-06-01'
    enterprise_user = 'data.analytics@koddi.com'
    #enterprise_token = 'bd7582800821623d44d2f9a59d4607f5d1316e6f'
    enterprise_host = 'https://app.koddi.com'

    db_username = 'reportrunner'
    #db_password = 'R3p0rtRunn3r8139'
    db_database = 'datamart'
    db_url = 'prod-reporting.travelhook.com'

    metrics = ['bookings', 'roomNights', 'revenue']

    sns_success_arn = 'arn:aws:sns:us-east-1:836434807709:hilton-advance-dashboard-comparison-success'
    sns_failure_arn = 'arn:aws:sns:us-east-1:836434807709:hilton-advance-dashboard-comparison-failure'

    output_bucket = 'travel-prod-monitoring-us-east-1'
    output_path = 'advance-dashboard/dt={}/comparison.csv'

    comparer = Comparer(site_id=site_id,
                        site_company_group_id=site_company_group_id,
                        start_date = start_date,
                        end_date=end_date,
                        enterprise_user=enterprise_user,
                        enterprise_token=enterprise_token,
                        enterprise_host=enterprise_host,
                        db_username=db_username,
                        db_password=db_password,
                        db_database=db_database,
                        db_url=db_url,
                        metrics=metrics,
                        sns_success_arn=sns_success_arn,
                        sns_failure_arn=sns_failure_arn)

    comparer.run(output_bucket, output_path)
