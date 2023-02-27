

from simple_salesforce import Salesforce
import requests
import pandas as pd
import numpy as np
import math

from io import StringIO
from datetime import datetime

BORROWER_IDS = {
    'Meridian Commodities Ltd':'0012u00000EmcLFAAZ',
    'Prometey Group':'0012u00000M2vzrAAB',
    'Usina Sao Domingos S.A':'0012u000009aJi6AAE',
    'Fiagril Ltda':'0012u00000336JnAAI',
    'Usina Santa Fe S.A.':'0012v00002Klue4AAB',
    'Premium Tobacco International DMCC':'0012v00002KlvKyAAJ',
    'Tabacos Marasca Ltda':'0012v00002oM1BjAAK',
    'Bevap':'0012v00002Qfb4xAAB'
}

FACILITY_LIMITS = {
    'Meridian Commodities Ltd':12500000,
    'Prometey Group':10000000,
    'Usina Sao Domingos S.A':8500000,
    'Fiagril Ltda':12500000,
    'Usina Santa Fe S.A.':15000000,
    'Premium Tobacco International DMCC':15000000,
    'Tabacos Marasca Ltda':5000000,
    'Bevap':12500000
}

def main():

    #sf = Salesforce(username='cthompson@almastone.com',password='Ktr321ugh!', security_token='')

    total_portfolio_limit = sum(FACILITY_LIMITS.values())

    data_df = pd.read_excel("test_salesforce.xlsx")
    data_df = data_df.loc[data_df['Borrower__c'].isin(BORROWER_IDS.values())]
    today_date = datetime.today()
    start_date = datetime.strptime('2022/9/01', '%Y/%m/%d')
    date_range = pd.date_range(start=start_date, end=today_date, freq='D')
    new_df = pd.DataFrame(date_range, columns=['Date'])

  
    e_code = ["Borrower_s_ESG_Risk_Categorisation_Score__c","ESG_Policy_Score__c","ESG_Certification_Score__c"]
    s_code = ["Code_of_Conduct_Ethics_Score__c","Human_Resources_Policy_Score__c","Responsible_ESG_Team_Score__c"]
    g_code = ["X3_Years_Audited_Fin_Statements_Score__c", "Audited_fin_statements_unqualified_Score__c", "Formal_hedging_risk_policy_Score__c", "monthly_quarterly_management_accts_Score__c", "High_Concentration_of_Power_Score__c","Single_customer_25_revenue_Score__c","Single_Creditor_25_obilgations_Score__c"]
    
    final_esg_ls = []
    final_g_ls = []
    final_s_ls = []
    final_e_ls = []

    for items, rows in new_df.iterrows():
        temp_date = rows.Date
        temp_e_ls = []
        temp_s_ls = []
        temp_g_ls = []
        temp_esg_ls = []
        for keys in BORROWER_IDS.keys():
            
            temp_df = data_df.loc[data_df['Borrower__c'].isin([BORROWER_IDS[keys]])]
            temp_weight = FACILITY_LIMITS[keys] / total_portfolio_limit
            temp_df = temp_df.loc[temp_df['CreatedDate'] <= temp_date]

            if temp_df.shape[0] == 0:
                continue
            else:
                temp_df = temp_df.loc[temp_df['CreatedDate'] == max(temp_df['CreatedDate'])]            
                e_score = sum(temp_df.filter(e_code, axis = 1).values[0]*100)
                g_score = sum(temp_df.filter(g_code, axis = 1).values[0]*100)
                s_score = sum(temp_df.filter(s_code, axis = 1).values[0]*100)
                esg_score = e_score + s_score + g_score
                temp_e_ls.append(e_score * temp_weight)
                temp_g_ls.append(g_score * temp_weight)
                temp_s_ls.append(s_score * temp_weight)
                temp_esg_ls.append(esg_score * temp_weight)

        try:
            final_esg_ls.append(round(sum(temp_esg_ls), 2))
            final_e_ls.append(round(sum(temp_e_ls), 2))
            final_s_ls.append(round(sum(temp_s_ls), 2))
            final_g_ls.append(round(sum(temp_g_ls), 2))
        except:
            final_esg_ls.append(0)
            final_e_ls.append(0)
            final_s_ls.append(0)
            final_g_ls.append(0)

    new_df['Environmental'] = final_e_ls
    new_df['Social'] = final_s_ls
    new_df['Governance'] = final_g_ls
    new_df['ESG Total Score'] = final_esg_ls
    
    return new_df

x_df = main()
x_df.to_excel('score_list_new.xlsx', index=False)