from src.util import ReportingUtil, Report
from src.util import APICall, APIParameter
from src.util import Database, DatabaseParameter
import pandas as pd
import numpy as np

site_id = 136
site_company_group_id = 28
start_date = '2019-06-01'
end_date = '2019-06-01'
enterprise_user = 'data.analytics@koddi.com'
enterprise_token = 'bd7582800821623d44d2f9a59d4607f5d1316e6f'
enterprise_host = 'https://app.koddi.com'

db_username = 'reportrunner'
db_password = 'R3p0rtRunn3r8139'
db_database = 'datamart'
db_url = 'prod-reporting.travelhook.com'

metrics = ['bookings', 'roomNights', 'revenue', 'consumedRevenue']


def get_enterprise_data(start_date, end_date, site_id=136):
    publisher = 'other_publishers'
    fields = ['transactions', 'profit', 'custom_decimal_4', 'custom_integer_3']
    dimensions = ['sub_placement', 'placement', 'report_date', 'hotel_id', 'hotel_chain']
    filters = [{'field': 'hotel_country', 'operation': 'in', 'value': 'US,CA', 'or_group': 'AND'}]

    report = Report(
            fields, dimensions, filters, start_date, end_date, site_id, publisher)

    res = ReportingUtil({
                "user": enterprise_user,
                "token": enterprise_token,
                "host": enterprise_host
            }).get_data(report)

    print(res)

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

    #df_enterprise['date'] = pd.to_datetime(df_enterprise['date'], errors='coerce')

    for m in metrics:
        df_enterprise[m] = pd.to_numeric(df_enterprise[m], errors='coerce')

    return df_enterprise


def get_pm_data(start_date, end_date, site_company_group_id=28):
    db_parameter = DatabaseParameter(db_url, db_username, db_password, db_database)
    db = Database(db_parameter)

    with open('queries/prodreporting.sql') as f:
        query = f.read()

    query = query.format(f"'{start_date}'", f"'{end_date}'", site_company_group_id)
    df = db.select_into_df(query)

    return df



enterprise = get_enterprise_data(start_date, end_date)
pm = get_pm_data(start_date, end_date)


print(enterprise.dtypes)
print(pm.dtypes)

print(enterprise)
print(pm)



compare = enterprise.merge(pm,
                           how='outer',
                           on=['hotelCode', 'date', 'channelName', 'subChannelName'],
                           suffixes=('_enterprise', '_pm'))


compare['hasDiff'] = 0
for m in metrics:
    compare[f'{m}_enterprise'] = compare[f'{m}_enterprise'].fillna(0)
    compare[f'{m}_pm'] = compare[f'{m}_pm'].fillna(0)
    compare[f'{m}_diff'] = compare[f'{m}_enterprise'] - compare[f'{m}_pm']
    compare['hasDiff'] = np.where(compare[f'{m}_diff'] == 0, compare['hasDiff'], 1)


cols = ['hotelCode', 'date', 'channelName', 'subChannelName', 'brand_enterprise', 'bookings_enterprise',
       'roomNights_enterprise', 'revenue_enterprise', 'consumedRevenue_enterprise', 'brand_pm', 'bookings_pm',
       'roomNights_pm', 'revenue_pm', 'consumedRevenue_pm', 'bookings_diff', 'roomNights_diff', 'revenue_diff',
       'consumedRevenue_diff', 'hasDiff']
compare = compare[cols]

compare.to_csv('out/compare_2019_06_01.csv', index=False)