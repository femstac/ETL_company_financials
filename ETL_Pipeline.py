# Import necessary libraries

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc
import pandas as pd
import os
import configparser as cp



# Read credentials from Config file 
config= cp.ConfigParser()
config.read('db_config.ini')

pwd = config['secrets']['PGSQL_Comp_Fin_PASS']
uid = config['secrets']['PGSQL_Comp_Fin_UID']


# Database Variables
server = "localhost"
db = "test_Company_Financials"
port = "5432"
file_dir= "C:\\Users\\USER\\Documents\\Python Scripts\\ETL Company Financials"
tbl_name = "Company_Financials_table"


file_dir= "C:\\Users\\USER\\Documents\\Python Scripts\\ETL Company Financials"




# Pipeline

#Extract Function
def extract_data_csv(csv_file):
    try:
        df= pd.read_csv(csv_file)
        return df
    
    except Exception as e:
        print('Cant find file')
         

    
    
    
    
# Transform Function

# Transform Function

integer_columns= 'Units Sold'

float_columns = ['Manufacturing Price', 'Sale Price', 'Gross Sales', 'Discounts', 'Sales', 'COGS', 'Profit']

date_columns = 'Date'

non_essential_columns = ['Month Number', 'Month Name', 'Year'] 

Dimension_Columns = ['Segment','Country', 'Product']





def transform_data_csv(extracted_data):
    try:
        df = extracted_data.copy()
        
        #select all the columns with string and object data types
        df_obj = df.select_dtypes(['object','string'])
        
        #using the strip method to remove all trailing zeroes in the object columns
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        
        
        # Remove the leading and trailing spaces in the columns names
        df.columns = df.columns.astype(str)
        df.columns = df.columns.str.strip()
        
        
        #Remove and/or Replace the following special characters in the tables '$', '-', ','
        df = df.replace ({'\$-':'0', ',':'','\$':'', '\(':'-', '\)':''}, regex=True)
        
        
        #Change data types
        df[date_columns] = pd.to_datetime(df[date_columns])
        df[float_columns]= df[float_columns].astype(float)
        df[integer_columns] = df[integer_columns].astype(float)
        
        
        # drop columns
        df = df.drop(non_essential_columns, axis=1)
        
        
              
        df['Discount per Unit']= df['Discounts'] / df['Units Sold']

        df['Percentage Profit'] = ((df['Profit'] / df['COGS']) *100).round(2)
        df['Unit Cost of Production'] = (df['COGS'] / df['Units Sold'])
        df = df.rename(columns={'Manufacturing Price': 'Unit Manufacturing Cost', 'Sale Price':'Unit Sale price'})
        
        
        # Get the unique values from the 'Discount Band' column
        discount_bands = df['Discount Band'].unique().tolist()
        discount_codes = {item: i+1 for i, item in enumerate(discount_bands)}
        
        
        def item_code(df, column_name):
            unique_items = df[column_name].unique()
            item_codes = {item: i+1 for i, item in enumerate(sorted(unique_items))}
            return item_codes
        
        

        for Dim_col in Dimension_Columns:
            df[f'{Dim_col} ID'] = df[Dim_col].apply(lambda x: item_code(df, Dim_col)[x])


        Segment_Dim = df[['Segment ID', 'Segment']].sort_values(by='Segment ID').drop_duplicates().reset_index(drop=True)

        Country_Dim = df[['Country ID', 'Country']].sort_values(by='Country ID').drop_duplicates().reset_index(drop=True)

        Product_Dim = df[['Product ID', 'Product']].sort_values(by='Product ID').drop_duplicates().reset_index(drop=True)


        Discount_Band_Dim = pd.DataFrame(discount_bands, columns=['Discount Band'])
        Discount_Band_Dim['Discount Band ID'] = Discount_Band_Dim['Discount Band'].apply(lambda x: discount_bands.index(x)+1)

        Discount_Band_Dim = Discount_Band_Dim[['Discount Band ID', 'Discount Band']]
         
    
        df['Discount Band ID'] = df['Discount Band'].apply(lambda x: discount_codes[x])
        
        tuples_tables = df, Segment_Dim, Country_Dim, Product_Dim, Discount_Band_Dim
        
        return  tuples_tables
        
    except Exception as e:
        print('failed to transform')

        
# Load Function
def load_csv_data(transformed_tables):
    try:
 
        df, Segment_Dim, Country_Dim, Product_Dim, Discount_Band_Dim = transformed_tables
        
        rows_imported = 0
        
        table_names = [tbl_name, 'Segments_table', 'Country_table', 'Product_table', 'Discount_Band_table']
        
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
        
        for table, name in zip(transformed_tables, table_names):
            print (f' Importing {len(table)} rows to {name} table...')
            
            rows_imported +=len(table)
            
            table.to_sql(name, engine, if_exists= 'replace', index=False)
            
    
    except Exception as e: 
        print('Failed to Load')
            
           
            
 
