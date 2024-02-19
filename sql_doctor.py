import pandas as pd

def get_org_id(input_org):
    df = pd.read_csv('orgs_data.csv')

    org_id = ""

    if input_org in df['brand'].values:
        org_id = df.loc[df['brand'] == input_org, 'org_id'].values[0]
    return org_id

def curated_sql(sql, input_org = 'Blackberrys'):
    org_id = get_org_id(input_org)
    curated_sql = f'use read_api_{org_id};' + sql
    return curated_sql
