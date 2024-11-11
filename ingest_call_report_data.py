import pandas as pd
import yaml
import pdb
import hashlib
import os
import numpy as np

class IngestCallReportData:
    def __init__(self):
        super().__init__()
        
    def process_files(self, files, dated_directory):
        final_df = pd.DataFrame()
        data_dictionary = {}
        invalid_rows_all = []
        for file in files:
            fp = f'data/ffiec_data/call_reports/{dated_directory}/{file}'
            df = pd.read_csv(fp, delimiter='\t')
            data = df.iloc[0].reset_index()
            data = data.drop(index=0)
            data = data.set_index('index')[0].to_dict()
            df = df.drop(index=0)
            unnamed_columns = [x for x in df.columns if 'Unnamed' in x]
            duplicate_columns = [x for x in df.columns if x in final_df.columns and x != 'IDRSSD']
            df = df.drop(columns=unnamed_columns + duplicate_columns)
            try:
                # pdb.set_trace()
                df['IDRSSD'] = pd.to_numeric(df['IDRSSD'], errors='coerce')
                invalid_rows = df[df['IDRSSD'].isna()]
                invalid_rows_all.append(invalid_rows)
                df = df.dropna(subset=['IDRSSD'])
                df['IDRSSD'] = df['IDRSSD'].astype(int).astype(str)
            except Exception as e:
                print(e)
                pdb.set_trace()
            if final_df.empty:
                final_df = df  
            else:
                final_df = pd.merge(final_df, df, how='outer', on='IDRSSD')
        final_df['date'] = dated_directory
        return final_df, data_dictionary, invalid_rows_all
        
    def read_data(self):
        final_df = pd.DataFrame()
        data_dictionary = {}
        invalid_rows_all = []
        directories_by_date = os.listdir('data/ffiec_data/call_reports')
        for dated_directory in directories_by_date:
            files = os.listdir(f'data/ffiec_data/call_reports/{dated_directory}')
            files = [x for x in files if 'Readme' not in x]
            df, data_dictionary, invalid_rows = self.process_files(files, dated_directory)
            final_df = pd.concat([final_df, df])
            data_dictionary.update(data_dictionary)
            invalid_rows_all.extend(invalid_rows)
        pdb.set_trace()
            

    
    def read_ingested_data(self):
        '''
        Just to start building the dashboard, I'm going to start preparing the elements of the dashboard
        Fields to keep:
            - IDRSSD
            KRIs: 
            - Cash items in process of collection, unposted debits, and currency and coin
                - SPLIT INTO:
                    - RCON0020 - Cash items in process of collection and unposted debits.
                    - RCON0080 - Currency and coin
                    
            - RCON0082 - Balances due from depository institutions in the U.S.
            - RCON0070 - Balances due from banks in foreign countries and foreign central banks
            - RCON0090 - Balances due from Federal Reserve Banks.
            
            - Cash and balances due from depository institutions (from Schedule RC-A):
                - SPLIT INTO:
                    - RCON0081 - Noninterest-bearing balances and currency and coin
                        - High RCON0081 levels could indicate that the bank serves many 
                          cash-intensive businesses, such as retail stores, restaurants, 
                          or other businesses where transactions are frequently in cash.
                    - RCON0071 - Interest-bearing balances
            ----------------------------------------------------------------------
            ----------------------------------------------------------------------
            DEPOSITS 
            ----------------------------------------------------------------------
            ----------------------------------------------------------------------
            - RCON2200 - Deposits In domestic offices (sum of totals of columns A and C from Schedule RC-E): 
                - SPLIT INTO:   
                    - RCON6631 - Noninterest-bearing
                    - RCON6636 - Interest-bearing
            - RCFN2200 - Deposits IN foreign offices, Edge and Agreement subsidiaries, and IBFs
                - SPLIT INTO:   
                    - RCFN6631 - Noninterest-bearing
                    - RCFN6636 - Interest-bearing
            ----------------------------------------------------------------------
            - Noninterest to Interest Ratio: High ratios may suggest cash-based transactions.
                - df['RCON6631'] / df['RCON6636']
                - Higher the ratio, higher the cash-based transactions
            - Foreign to Domestic Ratio: High foreign deposits can indicate cross-border exposure.
                - deposits in foreign offices / deposits in domestic offices
                - Higher the ratio, higher the cross-border exposure (more foreign deposits)
                - df['RCFN6631'] + df['RCFN6636']) / (df['RCON6631'] + df['RCON6636'])
            - Quarterly Growth in Noninterest Deposits: Monitors for abnormal increases in cash activity.
                - Check for sudden increase/decrease in noninterest domestic/foreign deposits (RCON6631 + RCFN6631)
                - Need quarterly historical data to calculate
                - df['quarterly_growth_rate'] = df['RCON6631'].pct_change()
            - Noninterest Proportion of Total Deposits: High proportions may indicate transient or cash-intensive accounts.
                - Calculate the proportion of noninterest-bearing deposits to total deposits to identify cash-intensive accounts.
                - df['total_deposits'] = df['RCON6631'] + df['RCON6636'] + df['RCFN6631'] + df['RCFN6636'], 
                    - (should be same as RCON2200 + RCFN2200)
                - df['noninterest_to_total_ratio'] = (df['RCON6631'] + df['RCFN6631']) / df['total_deposits']
                
            - FDIC Certificate Number	
            - OCC Charter Number	
            - OTS Docket Number	
            - Primary ABA Routing Number	
            - Financial Institution Name	
            - Financial Institution Address	
            - Financial Institution City	
            - Financial Institution State	
            - Financial Institution Zip Code  
        '''
        columns_to_keep = [
            'IDRSSD',
            'RCON0020', # Cash items in process of collection and unposted debits
            'RCON0080', # Currency and coin
            'RCON0082', # Balances due from depository institutions in the U.S.
            'RCON0070', # Balances due from banks in foreign countries and foreign central banks
            'RCON0090', # Balances due from Federal Reserve Banks
            'RCON0081', # Noninterest-bearing balances and currency and coin
            'RCON0071', # Interest-bearing balances
            
            'RCON2170', # Total Assets (Banks with assets less than $300 million)
            'RCFD2170', # Total Assets (Banks with assets greater than $300 million)
            
            'RCON3210', # Total Equity Capital (Banks with assets less than $300 million)
            'RCFD3210', # Total Equity Capital (Banks with assets greater than $300 million)
            
            'RCON2200', # Deposits In domestic offices
            'RCON6631', # Noninterest-bearing deposits in domestic offices
            'RCON6636', # Interest-bearing deposits in domestic offices
            
            'RCFN2200', # Deposits IN foreign offices, Edge and Agreement subsidiaries, and IBFs
            'RCFN6631', # Noninterest-bearing deposits in foreign offices
            'RCFN6636', # Interest-bearing deposits in foreign offices
           
        ]
        ########################
        df = pd.read_csv('data_all.csv',
                         usecols=columns_to_keep,
                         dtype={'IDRSSD': int})
        columns_to_convert = [x for x in df.columns if x != 'IDRSSD']
        df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
        
        pdb.set_trace()
        return df
    
    # df[df['RCFAA223'].notnull()]['RCFAA223']
    
if __name__ == '__main__':
    icrd = IngestCallReportData()
    icrd.read_ingested_data()
    
    
        # # pdb.set_trace()
        # # Consolidate Tier 1 and Tier 2 capital by adding foreign and domestic values, using fillna(0) for missing values
        # df['consolidated_tier_1'] = df['RCFA8274'].fillna(0) + df['RCOA8274'].fillna(0)
        # df['consolidated_tier_2'] = df['RCFA5311'].fillna(0) + df['RCOA5311'].fillna(0)

        # # Consolidate Risk-Weighted Assets
        # df['risk_weighted_assets'] = df['RCFAA223'].fillna(0) + df['RCOAA223'].fillna(0)

        # # Consolidate Total Assets
        # df['total_assets'] = df['RCFD2170'].fillna(0) + df['RCON2170'].fillna(0)

        # # Consolidate Total Equity Capital
        # df['total_equity_capital'] = df['RCFD3210'].fillna(0) + df['RCON3210'].fillna(0)

        # # Calculate Capital Adequacy Ratio (CAR) using consolidated Tier 1 and Tier 2 Capital
        # df['capital_adequacy_ratio'] = (df['consolidated_tier_1'] + df['consolidated_tier_2']) / df['risk_weighted_assets']

        # # Calculate Leverage Ratio using consolidated Tier 1 and total assets
        # df['leverage_ratio'] = df['consolidated_tier_1'] / df['total_assets']

        # # Calculate Equity to Assets Ratio using consolidated equity and total assets
        # df['equity_to_assets_ratio'] = df['total_equity_capital'] / df['total_assets']

        # # Replace infinity values in case of division by zero
        # df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # created_columns = [
        #     'consolidated_tier_1', 
        #     'consolidated_tier_2', 
        #     'risk_weighted_assets', 
        #     'total_assets', 
        #     'total_equity_capital', 
        #     'capital_adequacy_ratio', 
        #     'leverage_ratio', 
        #     'equity_to_assets_ratio'
        # ]
        
        # df.sort_values(['equity_to_assets_ratio'])[['IDRSSD'] + columns_to_keep]