from datetime import datetime

from sqlalchemy import create_engine
from airflow.decorators import task, dag
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import logging
from typing import Dict, Any
from pathlib import Path

# Configuration for all data sources
DATA_SOURCES = {
    'customer_profile': {
        'file_path': '/opt/airflow/data/interim/telco_churn_dirty.csv',
        'primary_key': 'customer_id',
    },
    'demographics': {
        'file_path': '/opt/airflow/data/interim/customer_demographics_dirty.csv',
        'primary_key': 'customer_id',
    },
    'location': {
        'file_path': '/opt/airflow/data/interim/customer_location_dirty.csv',
        'primary_key': 'customer_id',
    },
    'population': {
        'file_path': '/opt/airflow/data/interim/area_population_dirty.csv',
        'primary_key': 'area_code',
    },
    'services': {
        'file_path': '/opt/airflow/data/interim/customer_services_dirty.csv',
        'primary_key': 'customer_id',
    },
    'status': {
        'file_path': '/opt/airflow/data/interim/account_status_dirty.csv',
        'primary_key': 'customer_id',
    }
}

CONFIG = {
    'mysql_conn_id': 'mysql_telco_conn',
    'processed_data_path': '/opt/airflow/data/processed/'
}

def clean_Customer_ID_col(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize columns
    for col in df.columns:
        df[col] = df[col].strip().lower()

    df = df[df['Customer ID'].str.match(r'^\d{4}-[A-Z]{5}$', na=False)]     # must match this *exact* pattern
    df = df.drop_duplicates(subset=['Customer ID'])    # gotta be unique since its (almost always) our primary key
    return df

@dag(
    dag_id='telco_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'telco churn', 'multi-source'],
    doc_md=__doc__
)
def telco_etl():
    """
    Multi-source ETL pipeline for telco data
    Processes: churn, demographics, location, population, services, status
    """
    @task
    def extract_and_validate_source(source_name: str) -> Dict[str, Any]:
        """Extract and validate a single data source"""
        config = DATA_SOURCES[source_name]
        logging.info(f"Extracting {source_name} from {config['file_path']}")
        
        # Read CSV
        df = pd.read_csv(config['file_path'])
        
        # Basic validation
        if df.empty:
            raise ValueError(f"{source_name} dataset is empty")
        
        # Data quality assessment
        missing_count = df.isnull().sum().sum()
        total_cells = len(df) * len(df.columns)
        quality_score = (total_cells - missing_count) / total_cells
        
        # Check for primary key issues
        pk_column = config['primary_key']
        pk_issues = 0
        if pk_column in df.columns:
            pk_nulls = df[pk_column].isnull().sum()
            pk_duplicates = df[pk_column].duplicated().sum()
            pk_issues = pk_nulls + pk_duplicates
        
        validation_result = {
            'source': source_name,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'primary_key_issues': pk_issues,
            'missing_values': int(missing_count),
            'quality_score': f"{quality_score:.2%}"
        }
        
        # Log validation results (for later comparison)
        logging.info(f"{validation_result}")

        return df
    

    @task
    def transform_customer_profile_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer profile data from churn csv"""
        original_rows = len(df)

        df = clean_Customer_ID_col(df)

        # Clean tenure field
        df['Tenure'] = df['Tenure'].str.strip().str.strip("$").str.replace(' months', '')
        df['Tenure'] = pd.to_numeric(df['Tenure'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Customer ID', 'Tenure'])
        df['Tenure'] = df['Tenure'].astype(int)
        
        # Save cleaned file
        cleaned_path = Path(CONFIG['processed_data_path']) / 'churn_cleaned.csv'
        df[['Customer ID', 'Tenure']].to_csv(cleaned_path, index=False)

        logging.info(f"Customer profile data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'Tenure': 'tenure_months'})
        return df[['customer_id', 'tenure_months']]

    @task
    def transform_demographics_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform demographics data"""
        original_rows = len(df)

        df = clean_Customer_ID_col(df)

        # Clean Age column
        df['Age'] = df['Age'].str.strip("$")
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')

        # Clean categorical column
        df = df[df['Gender'].isin(['male', 'female'])]

        # Clean the messy Boolean columns using mapping
        df['Married'] = df['Married'].map(
            lambda x: 'yes' if x in {'yes', 'partnered', 'married'} else ('no' if x in {'no', 'single', 'unmarried'} else pd.NA)
        )
        df = df[df['Dependents'].map(
            lambda x: 'yes' if x in {'yes', 'children', 'kids', 'family'} else ('no' if x in {'no', 'false', '0', 'none'} else pd.NA)
        )]

        df['Number of Dependents'] = pd.to_numeric(df['Number of Dependents'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Customer ID', 'Gender', 'Age', 'Under 30', 'Senior Citizen', 'Married', 'Dependents', 'Number of Dependents'])
        df['Age'] = df['Age'].astype(int)
        df['Number of Dependents'] = df['Number of Dependents'].astype(int)

        cleaned_path = Path(CONFIG['processed_data_path']) / 'demographics_cleaned.csv'
        df[['Customer ID', 'Gender', 'Age', 'Under 30', 'Senior Citizen', 'Married', 'Dependents', 'Number of Dependents']].to_csv(cleaned_path, index=False)

        logging.info(f"Demographics data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'Under 30': 'is_under_30', 'Senior Citizen': 'is_senior_citizen', 'Married': 'has_partner', 'Dependents': 'has_dependents', 'Number of Dependents': 'number_of_dependents'})
        return df[['customer_id', 'Gender', 'Age', 'is_under_30', 'is_senior_citizen', 'has_partner', 'has_dependents', 'number_of_dependents']]

    @task
    def transform_location_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform location data"""
        original_rows = len(df)

        df = clean_Customer_ID_col(df)

        # Clean Zip Code column
        df['Zip Code'] = df['Zip Code'].str.strip('$').str.replace(',', '')
        df['Zip Code'] = pd.to_numeric(df['Zip Code'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Customer ID', 'City', 'Zip Code'])
        df['Zip Code'] = df['Zip Code'].astype(int)
        
        # Save cleaned file
        cleaned_path = Path(CONFIG['processed_data_path']) / 'location_cleaned.csv'
        df[['Customer ID', 'City', 'Zip Code']].to_csv(cleaned_path, index=False)

        logging.info(f"Location data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'City': 'city', 'Zip Code': 'zip_code'})
        return df[['customer_id', 'city', 'zip_code']]

    @task
    def transform_population_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform population data"""
        original_rows = len(df)

        # Standardize columns
        for col in df.columns:
            df[col] = df[col].strip().lower()
        df = df.drop_duplicates(subset=['Zip Code'])    # gotta be unique since its our primary key

        # Clean Zip Code column
        df['Zip Code'] = df['Zip Code'].str.strip('$').str.replace(',', '')
        df['Zip Code'] = pd.to_numeric(df['Zip Code'], errors='coerce')

        # Clean Population column
        df['Population'] = df['Population'].str.strip('$').str.replace(',', '')
        df['Population'] = pd.to_numeric(df['Population'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Zip Code', 'Population'])
        df['Zip Code'] = df['Zip Code'].astype(int)
        df['Population'] = df['Population'].astype(int)

        # Save cleaned file
        cleaned_path = Path(CONFIG['processed_data_path']) / 'population_cleaned.csv'
        df[['Zip Code', 'Population']].to_csv(cleaned_path, index=False)

        logging.info(f"Population data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Zip Code': 'zip_code', 'Population': 'population'})
        return df[['zip_code', 'population']]

    @task
    def transform_services_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform services data"""
        original_rows = len(df)
        df_fields = ['Customer ID', 'Referred a Friend', 'Number of Referrals', 'Phone Service', 'Multiple Lines', 'Internet Service', 'Internet Type', 'Online Security', 'Online Backup', 'Avg Monthly Long Distance Charges', 'Avg Monthly GB Download', 'Online Security', 'Online Backup', 'Device Protection Plan', 'Premium Tech Support', 'Streaming TV', 'Streaming Movies', 'Streaming Music', 'Unlimited Data']

        df = clean_Customer_ID_col(df)

        # Clean boolean columns
        df['Phone Service'] = df['Phone Service'].map(lambda x: 'yes' if x in {'yes', 'active', 'enabled'} else ('no' if x in {'no', 'none'} else pd.NA))
        df['Multiple Lines'] = df['Multiple Lines'].map(lambda x: 'yes' if x in {'yes', 'multiple'} else ('no' if x in {'no', 'none'} else pd.NA))
        df['Internet Service'] = df['Internet Service'].map(lambda x: 'yes' if x == 'yes' else ('no' if x in {'no', 'none'} else pd.NA))
        df['Online Security'] = df['Online Security'].map(lambda x: 'yes' if x in {'yes', 'enabled'} else ('no' if x in {'no', 'none'} else pd.NA))
        df['Online Backup'] = df['Online Backup'].map(lambda x: 'yes' if x in {'yes', 'enabled'} else ('no' if x in {'no', 'none'} else pd.NA))

        # Clean categorial column
        df['Internet Type'] = df['Internet Type'].map(lambda x: x if x in {'dsl', 'fiber optic'} else ('none' if x == '' else pd.NA))

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=[df_fields])
        df['Number of Referrals'] = df['Number of Referrals'].astype(int)
        df['Avg Monthly Long Distance Charges'] = df['Avg Monthly Long Distance Charges'].astype(int)
        df['Avg Monthly GB Download'] = df['Avg Monthly GB Download'].astype(int)

        cleaned_path = Path(CONFIG['processed_data_path']) / 'services_cleaned.csv'
        df[df_fields].to_csv(cleaned_path, index=False)

        logging.info(f"Services data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'Referred a Friend': 'has_referred_a_friend', 'Number of Referrals': 'number_of_referrals', 'Phone Service': 'has_phone_service', 'Multiple Lines': 'has_multiple_lines', 'Internet Service': 'has_internet_service', 'Internet Type': 'internet_service_type', 'Online Security': 'has_online_security', 'Online Backup': 'has_online_backup', 'Avg Monthly Long Distance Charges': 'avg_monthly_long_distance_charges', 'Avg Monthly GB Download': 'avg_monthly_gb_download', 'Device Protection Plan': 'has_device_protection', 'Premium Tech Support': 'has_tech_support', 'Streaming TV': 'has_tv', 'Streaming Movies': 'has_movies', 'Streaming Music': 'has_music', 'Unlimited Data': 'has_unlimited_data'})
        return df[['customer_id', 'has_referred_a_friend', 'number_of_referrals', 'has_phone_service', 'has_multiple_lines', 'has_internet_service', 'internet_service_type', 'has_online_security', 'has_online_backup', 'avg_monthly_long_distance_charges', 'avg_monthly_gb_download', 'has_device_protection', 'has_tech_support', 'has_tv', 'has_movies', 'has_music', 'has_unlimited_data']]

    @task
    def transform_financials_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform financials data"""
        original_rows = len(df)

        df = clean_Customer_ID_col(df)

        # Clean Boolean column
        df['Paperless Billing'] = df['Paperless Billing'].map(lambda x: 'yes' if x in {'yes', 'electronic', 'digital'} else ('no' if x in {'no', 'paper', 'mail'} else pd.NA))

        # Clean numeric columns
        df['Total Charges'] = df['Total Charges'].str.strip("$").str.replace(',', '')
        
        numeric_cols = ['Monthly Charge', 'Total Charges', 'Total Refunds', 'Total Extra Data Charges', 'Total Long Distance Charges', 'Total Revenue']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Customer ID', 'Contract', 'Paperless Billing', 'Payment Method'] + numeric_cols)
        
        for col in numeric_cols:
            df[col] = df[col].astype(float)

        cleaned_path = Path(CONFIG['processed_data_path']) / 'financials_cleaned.csv'
        df[['Customer ID', 'Contract', 'Paperless Billing', 'Payment Method'] + numeric_cols].to_csv(cleaned_path, index=False)

        logging.info(f"Financials data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'Contract': 'contract_type', 'Paperless Billing': 'has_paperless_billing', 'Payment Method': 'payment_method', 'Monthly Charge': 'monthly_charges', 'Total Charges': 'total_charges', 'Total Refunds': 'total_refunds', 'Total Extra Data Charges': 'total_extra_data_charges', 'Total Long Distance Charges': 'total_long_distance_charges', 'Total Revenue': 'total_revenue'})
        return df[['customer_id', 'contract_type', 'has_paperless_billing', 'payment_method', 'monthly_charges', 'total_charges', 'total_refunds', 'total_extra_data_charges', 'total_long_distance_charges', 'total_revenue']]

    @task
    def transform_status_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform account status data"""
        original_rows = len(df)

        df = clean_Customer_ID_col(df)

        # Clean numerical columns
        num_cols = ['CLTV', 'Satisfaction Score', 'Churn Score']
        for col in num_cols:
            df[col] = df[col].str.strip('$').str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean categorial column
        df['Churn Category'] = df['Churn Category'].map(lambda x: x if x in {'competitor', 'dissatisfaction', 'attitude', 'price', 'other'} else ('not_specified' if x == '' else pd.NA))

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['Customer ID', 'Satisfaction Score', 'Churn Label', 'Churn Score', 'CLTV', 'Churn Category'])
        
        cleaned_path = Path(CONFIG['processed_data_path']) / 'status_cleaned.csv'
        df[['Customer ID', 'Satisfaction Score', 'Churn Label', 'Churn Score', 'CLTV', 'Churn Category']].to_csv(cleaned_path, index=False)

        logging.info(f"Status data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'Customer ID': 'customer_id', 'Satisfaction Score': 'satisfaction_score', 'Churn Label': 'churn_label', 'Churn Score': 'churn_score', 'CLTV': 'cltv', 'Churn Category': 'churn_category'})
        return df[['customer_id', 'satisfaction_score', 'churn_label', 'churn_score', 'cltv', 'churn_category']]

    @task
    def load_to_database(table_name: str, df: pd.DataFrame):
        """Load cleaned data to MySQL database"""

        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
        cnx = hook.get_conn()
        engine = create_engine(hook.get_uri(), creator=lambda: cnx)
       
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        engine.dispose()

        logging.info(f"Successfully loaded {len(df)} {table_name} records")
    
    # Extract all sources into dataframes
    extracted_dataframes = {}
    for source in DATA_SOURCES.keys():
        extracted_dataframes[source] = extract_and_validate_source(source)
    
    # Transform dataframes (dependency is implicit here since we are using the output of one task as input to another)
    transformed_dataframes = {}

    transformed_dataframes['customer_profile'] = transform_customer_profile_data(extracted_dataframes['customer_profile'])
    # transformed_dataframes['demographics'] = transform_demographics_data(extracted_dataframes['demographics'])

    # transformed_dataframes['location'] = transform_location_data(extracted_dataframes['location'])
    # transformed_dataframes['population'] = transform_population_data(extracted_dataframes['population'])

    # transformed_dataframes['services'] = transform_services_data(extracted_dataframes['services'])
    # transformed_dataframes['financials'] = transform_financials_data(extracted_dataframes['services'])

    # transformed_dataframes['status'] = transform_status_data(extracted_dataframes['status'])

    # # Load dataframes into database
    # load_customer_profile = load_to_database('customer_profile', Path(CONFIG['processed_data_path']) / 'churn_cleaned.csv')     # everything is dependent on customer profile data

    # # Load population and location with explicit dependency
    # load_population = load_to_database('population', transformed_dataframes['population'])
    # load_location = load_to_database('location', transformed_dataframes['location'])
    # load_population >> load_location

    # # Load remaining sources while keeping dependencies in mind
    # load_tasks = []
    # for name, df in transformed_dataframes.items():
    #     if name not in ['customer_profile', 'population', 'location']:
    #         load_task = load_to_database(name, df)
    #         load_tasks.append(load_task)
    #         # customer_profile must load before all other tables
    #         load_customer_profile >> load_task

    # load_customer_profile >> load_population
    # load_customer_profile >> load_location

# Instantiate the DAG
telco_etl()