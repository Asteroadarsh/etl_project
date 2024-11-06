import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import os

def read_csv(file_path):
    """
    Reads data from a CSV file and returns a Pandas DataFrame.
    """
    try:
        data = pd.read_csv(file_path)
        data.columns = data.columns.str.strip()  
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def clean_and_convert_date(date_str):
    """Cleans and converts date strings to the format DD/MM/YYYY."""
    try:
        if '-' in date_str:
            if date_str.count('-') == 2:  
                if len(date_str.split('-')[0]) == 4:  
                    return datetime.strptime(date_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                else:  
                    return datetime.strptime(date_str, '%d-%m-%Y').strftime('%d/%m/%Y')
    except ValueError:
        return None  

    return None  

def transform_data(df):
    """
    Transforms the data according to specified business rules.
    """
    df['CleanedBirthDate'] = df['BirthDate'].apply(clean_and_convert_date)

    reference_date = pd.to_datetime("2023-01-01")
    df['Age'] = df['CleanedBirthDate'].apply(lambda x: (reference_date - pd.to_datetime(x, format='%d/%m/%Y')).days // 365 if x is not None else None)

    df['FirstName'] = df['FirstName'].str.strip()
    df['LastName'] = df['LastName'].str.strip()

    df['FullName'] = df['FirstName'] + ' ' + df['LastName']

    df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce')
    df.dropna(subset=['Salary'], inplace=True) 

    def categorize_salary(salary):
        if salary < 50000:
            return 'A'
        elif 50000 <= salary <= 100000:
            return 'B'
        else:
            return 'C'

    df['SalaryBucket'] = df['Salary'].apply(categorize_salary)

    df.drop(['FirstName', 'LastName','CleanedBirthDate','BirthDate'], axis=1, inplace=True)

    return df

def load_data(df, db_url, table_name):
    """
    Loads transformed data into a PostgreSQL database.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists='replace', index=False)
            print(f"Data successfully loaded into {table_name}")
    except Exception as e:
        print(f"Error loading data to the database: {e}")

if __name__ == "__main__":
    file_path = os.getenv('FILE_PATH', 'employee_details.csv')
    
    db_url = os.getenv('DATABASE_URL', 'postgresql://adarsh:zxcvbnm@db:5432/etl_db')
    
    df = read_csv(file_path)
    
    if df is not None:
        transformed_df = transform_data(df)
        
        load_data(transformed_df, db_url, 'employee_data')
