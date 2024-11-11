import pandas as pd
import yaml
import pdb
import hashlib


class IngestFFIECData:
    def __init__(self):
        self.hq_ffiec_fp = 'data/ffiec_data/CSV_ATTRIBUTES_ACTIVE.csv'
        self.branch_ffiec_fp = 'data/ffiec_data/CSV_ATTRIBUTES_BRANCHES.csv'
        
        self.hq_fdic_fp = 'data/fdic_data/institutions.csv'
        self.branch_fdic_fp = 'data/fdic_data/locations.csv'
        
        self.hq_ncua_fp = 'data/ncua_data/FOICU.txt'
        self.branch_ncua_fp = 'data/ncua_data/Credit Union Branch Information.txt'

        
        self.ffiec_branch_data_fp = 'data/ffiec_data/CSV_ATTRIBUTES_BRANCHES.csv'
        
        with open("mappings/hq_column_mapping.yaml", "r") as file:
            self.hq_column_mappings = yaml.safe_load(file)["hq_columns"]
        
        with open("mappings/branch_column_mapping.yaml", "r") as file:
            self.branch_column_mappings = yaml.safe_load(file)["branch_fields"]

        self.ffiec_hq_columns = [v["ffiec"] for _, v in self.hq_column_mappings.items() if v["ffiec"] != None] 
        self.ncua_branch_columns = [v["ncua"] for _, v in self.branch_column_mappings.items() if v["ncua"] != None]
        self.fdic_branch_columns = [v["fdic"] for _, v in self.branch_column_mappings.items() if v["fdic"] != None] 

    # Function to rename columns based on source
    def rename_columns(self, df, source, type):
        if type == 'branch':
            rename_dict = {v[source]: k for k, v in self.branch_column_mappings.items() if v[source]}
        elif type == 'hq':
            rename_dict = {v[source]: k for k, v in self.hq_column_mappings.items() if v[source]}
        return df.rename(columns=rename_dict)
                
    def generate_hash(self, df, columns):
        # Join specified columns for each row and hash the combined string
        return df[columns].astype(str).agg(''.join, axis=1).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

    def clean_ffiec_hq_data(self, df_ffiec_hq):
        '''
        We're looking for situations where the HQ_ID_RSSD, HQ_ID_OCC, HQ_ID_FDIC_CERT, 
        or HQ_ID_NCUA is not 0, or the HQ_PRIM_FED_REG is not 'FRB'. 
        If all fields are 0, and the HQ_PRIM_FED_REG is also not 'FRB', 
        then we filter out that record. 

        Will need to look at the record in more detail and see if we 
        can determine why the record is being filtered out.
        '''
        
        conditions_1 = df_ffiec_hq['occ_id'] == 0
        conditions_2 = df_ffiec_hq['fdic_id'] == 0
        conditions_3 = df_ffiec_hq['ncua_id'] == 0
        conditions_4 = df_ffiec_hq['primary_fed_reg'] != 'FRS'
        conditions = conditions_1 & conditions_2 & conditions_3 & conditions_4
        # Data to be filtered out, log this later
        # df_ffiec_hq_other = df_ffiec_hq[(conditions)]
        df_ffiec_hq = df_ffiec_hq[~(conditions)]
        df_ffiec_hq = df_ffiec_hq[[*self.hq_column_mappings.keys()]]
        return df_ffiec_hq 
        
    def load_ffiec_data(self):
        df_ffiec_hq = pd.read_csv(
            self.hq_ffiec_fp,
            usecols=self.ffiec_hq_columns)
            # usecols=['#ID_RSSD', 'ID_FDIC_CERT', 'ID_NCUA', 'ID_OCC', 'PRIM_FED_REG'])
        # df_ffiec_hq.columns = ['rssd_id', 'fdic_id', 'ncua_id', 'occ_id', 'primary_fed_reg']
        df_ffiec_hq = self.rename_columns(df_ffiec_hq, 'ffiec', 'hq')
        # pdb.set_trace()
        df_ffiec_branch = pd.read_csv(
            self.branch_ffiec_fp,
            usecols=['#ID_RSSD', 'ID_RSSD_HD_OFF', 'STREET_LINE1'])   
        df_ffiec_branch.columns = ['rssd_id', 'branch_id', 'street_address']
        
        df_ffiec_branch['branch_id'] = self.generate_hash(
            df_ffiec_branch, 
            ['branch_id', 'street_address'])
        return df_ffiec_hq, df_ffiec_branch
        
    def load_fdic_data(self):
        fdic_hq_columns = {
            'FED_RSSD': 'rssd_id',
            'CERT': 'fdic_id',
            'ACTIVE': 'active_status'
        }
        
        df_fdic_hq = pd.read_csv(
            self.hq_fdic_fp,
            usecols=[*fdic_hq_columns.keys()])
        df_fdic_hq.rename(columns=fdic_hq_columns, inplace=True)
        df_fdic_hq = df_fdic_hq[df_fdic_hq['active_status']==1]
        df_fdic_hq.drop(columns=['active_status'], inplace=True)
        
        df_fdic_branch = pd.read_csv(
            self.branch_fdic_fp,
            usecols=self.fdic_branch_columns)
        df_fdic_branch = self.rename_columns(df_fdic_branch, 'fdic', 'branch')
        df_fdic_branch['branch_id'] = self.generate_hash(
            df_fdic_branch, 
            ['branch_id', 'institution_name', 'address_line1'])
        # df_fdic_branch = df_fdic_branch.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # df_fdic_branch = df_fdic_branch[self.fdic_branch_columns + ['branch_id']]
        return df_fdic_hq, df_fdic_branch
         
    def load_ncua_data(self):
        ncua_hq_columns = {
            'CU_NUMBER': 'ncua_id',
            'RSSD': 'rssd_id'
        }
        df_ncua_hq = pd.read_csv(
            self.hq_ncua_fp,
            usecols=[*ncua_hq_columns.keys()])
        df_ncua_hq.rename(columns=ncua_hq_columns, inplace=True)
        
        df_ncua_branch = pd.read_csv(
            self.branch_ncua_fp,
            usecols=self.ncua_branch_columns)
        df_ncua_branch = self.rename_columns(df_ncua_branch, 'ncua', 'branch')
        df_ncua_branch = df_ncua_branch[df_ncua_branch['branch_type'] == 'Branch Office']
        
        df_ncua_branch['branch_id'] = self.generate_hash(
            df_ncua_branch, 
            ['branch_id', 'institution_name', 'address_line1'])
        
        return df_ncua_hq, df_ncua_branch

    def clean_fdic_data(self, df_fdic_hq, df_fdic_branch):
        df_fdic = pd.merge(df_fdic_hq, df_fdic_branch, 
                           left_on='fdic_id', right_on='institution_id', how='left')
        df_fdic['source'] = 'fdic'
        df_fdic.drop_duplicates(['branch_id'], inplace=True) # log this
        return df_fdic
    
    def clean_ncua_data(self, df_ncua_hq, df_ncua_branch):
        df_ncua = pd.merge(df_ncua_hq, df_ncua_branch, 
                           left_on='ncua_id', right_on='institution_id', how='left')
        df_ncua['source'] = 'ncua'
        df_ncua.drop_duplicates(['branch_id'], inplace=True) # log this
        return df_ncua
        
    def run(self):
        df_ffiec_hq, _ = self.load_ffiec_data()
        df_fdic_hq, df_fdic_branch = self.load_fdic_data()
        df_ncua_hq, df_ncua_branch = self.load_ncua_data()  
        
        df_hq = self.clean_ffiec_hq_data(df_ffiec_hq)
        df_fdic = self.clean_fdic_data(df_fdic_hq, df_fdic_branch)
        df_ncua = self.clean_ncua_data(df_ncua_hq, df_ncua_branch)
        
        df_branch = pd.concat([df_fdic, df_ncua])
        df_branch = df_branch[[*self.branch_column_mappings.keys()]]
        df = pd.merge(df_hq, df_branch, on='rssd_id', how='left', indicator=True)
        pdb.set_trace()
        '''
        For now, I'm going to only use the df_fdic, df_ncua data for
        branch data. I'll use the df_ffiec_hq data for the HQ data.
        
        df_ffiec_hq: 
            - primary-key = rssd_id
        df_fdic:
            - primary_key = branch_id
            - foreign_key = rssd_id
        df_ncua:
            - primary_key = branch_id
            - foreign_key = rssd_id
            
        When the raw FDIC and NCUA data is loaded, it should automatically
        get that branch_id as a primary key.
        
        Right now, this will give the ability to join the FDIC and NCUA branch
        data with the FFIEC HQ data, however, eventually, I'd like to ER
        the data together between everything since there appears to be some
        things missing. 
        
        For now, we have two tables: 
            - HQ Data
            - Branch Data
            
        BUT we are missing all the fields between them. 
        
        Next steps:
        1) Ingest the Raw HQ Data into a table. 
        2) Ingest the Raw Branch Data into a table.
            - Apply a 'branch_id' to each row in the branch data
        3) Include logging elements to keep track of what is being filtered out
        4) Need to stack the FDIC and NCUA branch data 'on top' of each other
            - Need to standardize the column names.
            - This will be the 'branch' table
        5) Restart the above ingestion
        
        FINISHED ALL THE ABOVE STEPS. 
        Before Moving onto the below steps, I can get started on the actual transformations
        of what I want to do. 
        
        
        
        1) Only additional thing I really need to do transformation wise is:
            - Clean the Address fields for all datasets
            - I want to pull in additional fields from the FDIC and NCUA data (the codes)
              I want to put together some risk indicators USING this data for the visualziations. 
              
        
        Visualizations I want to do:
        - Table for All the 'HQ' data
            - You click on one, all the branches appear on the map. 
            - A network graph comes up of the relationship between the HQ and its holding companies.
            - Put up some basic graphs for the bank's financials.
            - Come up with some risk indicators using this data. 
            
        
        
        OtherEventual Next Steps:
        1) Clean the FDIC, NCUA, FFIEC addresses for all datasets
        2) Entity Resolution for branch data using the RSSD and address fields. 
           This will allow us to use the fields between both. 
            - FFIEC Branch -> FDIC Branch
            - FFIEC Branch -> NCUA Branch (NCUA branch data appears to be missing from FFIEC)
            - Maybe do the HQ data as well? 
        '''
        
if __name__ == '__main__':
    ingest = IngestFFIECData()
    ingest.run()    