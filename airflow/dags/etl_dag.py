from datetime import datetime

from sqlalchemy import create_engine
from airflow.decorators import task, dag
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import logging
from pathlib import Path

# Configuration for all data sources
DATA_SOURCES = {
    'customer_profile': {
        'file_path': '/opt/airflow/data/interim/churn_dirty.csv',
        'primary_key': 'customer_id',
    },
    'demographics': {
        'file_path': '/opt/airflow/data/interim/demographics_dirty.csv',
        'primary_key': 'customer_id',
    },
    'location': {
        'file_path': '/opt/airflow/data/interim/location_dirty.csv',
        'primary_key': 'customer_id',
    },
    'population': {
        'file_path': '/opt/airflow/data/interim/population_dirty.csv',
        'primary_key': 'area_code',
    },
    'services': {
        'file_path': '/opt/airflow/data/interim/services_dirty.csv',
        'primary_key': 'customer_id',
    },
    'status': {
        'file_path': '/opt/airflow/data/interim/status_dirty.csv',
        'primary_key': 'customer_id',
    }
}

CONFIG = {
    'mysql_conn_id': 'mysql_telco_conn',
    'processed_data_path': '/opt/airflow/data/processed/'
}

def clean_Customer_ID_col(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize columns
    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.applymap(lambda x: str(x).strip().lower() if pd.notnull(x) else x)

    df = df[df['customer id'].str.match(r'^\d{4}-[a-z]{5}$', na=False)]  # pattern should also be lowercase
    df = df.drop_duplicates(subset=['customer id'])
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
    def validate_source(source_name: str):
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
    

    @task
    def transform_customer_profile_data(file_path: str) -> str:
        """Transform customer profile data from churn csv"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned customer id column!")
        logging.info(f"{df.head()}")

        # Clean tenure field
        df['tenure'] = df['tenure'].str.strip().str.strip("$").str.replace(' months', '')
        df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['customer id', 'tenure'])
        df['tenure'] = df['tenure'].astype(int)

        logging.info(f"Customer profile data transformed: {len(df)}/{original_rows} rows retained")

        # Save cleaned file
        df = df.rename(columns={'customer id': 'customer_id', 'tenure': 'tenure_months'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'churn_cleaned.csv'
        df[['customer_id', 'tenure_months']].to_csv(cleaned_path, index=False)


    @task
    def transform_demographics_data(file_path: str) -> str:
        """Transform demographics data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned Customer ID column!")

        # Clean Age column
        df['age'] = df['age'].str.strip("$")
        df['age'] = pd.to_numeric(df['age'], errors='coerce')

        # Clean categorical column
        df = df[df['gender'].isin(['male', 'female'])]

        # Clean the messy Boolean columns using mapping
        df['married'] = df['married'].map(lambda x: 1 if x in {'yes', 'partnered', 'married'} else (0 if x in {'no', 'single', 'unmarried'} else pd.NA))
        df['dependents'] = df['dependents'].map(lambda x: 1 if x in {'yes', 'children', 'kids', 'family'} else (0 if x in {'no', 'false', '0', 'none'} else pd.NA))
        df['under 30'] = df['under 30'].map(lambda x: 1 if x == 'yes' else (0 if x == 'no' else pd.NA))
        df['senior citizen'] = df['senior citizen'].map(lambda x: 1 if x == 'yes' else (0 if x == 'no' else pd.NA))

        logging.info(f"{df.head()}") # DataFrame Empty
        df['number of dependents'] = pd.to_numeric(df['number of dependents'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['customer id', 'gender', 'age', 'under 30', 'senior citizen', 'married', 'dependents', 'number of dependents'])
        df['age'] = df['age'].astype(int)
        df['number of dependents'] = df['number of dependents'].astype(int)

        logging.info(f"Demographics data transformed: {len(df)}/{original_rows} rows retained")
        
        df = df.rename(columns={'customer id': 'customer_id', 'under 30': 'is_under_30', 'senior citizen': 'is_senior_citizen', 'married': 'has_partner', 'dependents': 'has_dependents', 'number of dependents': 'number_of_dependents'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'demographics_cleaned.csv'
        df[['customer_id', 'gender', 'age', 'is_under_30', 'is_senior_citizen', 'has_partner', 'has_dependents', 'number_of_dependents']].to_csv(cleaned_path, index=False)


    @task
    def transform_location_data(file_path: str) -> str:
        """Transform location data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned Customer ID column!")

        # Clean Zip Code column
        df['zip code'] = df['zip code'].str.strip('$').str.replace(',', '')
        df['zip code'] = pd.to_numeric(df['zip code'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['customer id', 'city', 'zip code'])
        df['zip code'] = df['zip code'].astype(int)

        logging.info(f"Location data transformed: {len(df)}/{original_rows} rows retained")

        # Save cleaned file
        df = df.rename(columns={'customer id': 'customer_id', 'zip code': 'zip_code'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'location_cleaned.csv'
        df[['customer_id', 'city', 'zip_code']].to_csv(cleaned_path, index=False)


    @task
    def transform_population_data(file_path: str) -> str:
        """Transform population data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        # Standardize columns
        df.columns = [str(col).strip().lower() for col in df.columns]
        df = df.applymap(lambda x: str(x).strip().lower() if pd.notnull(x) else x)
        df = df.drop_duplicates(subset=['zip code'])    # gotta be unique since its our primary key

        # Clean zip code column
        df['zip code'] = df['zip code'].str.strip('$').str.replace(',', '')
        df['zip code'] = pd.to_numeric(df['zip code'], errors='coerce')

        # Clean population column
        df['population'] = df['population'].str.strip('$').str.replace(',', '')
        df['population'] = pd.to_numeric(df['population'], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['zip code', 'population'])
        df['zip code'] = df['zip code'].astype(int)
        df['population'] = df['population'].astype(int)

        logging.info(f"population data transformed: {len(df)}/{original_rows} rows retained")

        # Save cleaned file
        df = df.rename(columns={'zip code': 'zip_code'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'population_cleaned.csv'
        df[['zip_code', 'population']].to_csv(cleaned_path, index=False)

    @task
    def transform_services_data(file_path: str) -> str:
        """Transform services data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)
        df_fields = ['customer id', 'referred a friend', 'number of referrals', 'phone service', 'multiple lines', 'internet service', 'internet type', 'avg monthly long distance charges', 'avg monthly gb download', 'online security', 'online backup', 'device protection plan', 'premium tech support', 'streaming tv', 'streaming movies', 'streaming music', 'unlimited data']

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned customer id column!")

        # Clean boolean columns
        df['phone service'] = df['phone service'].map(lambda x: 1 if x in {'yes', 'active', 'enabled'} else (0 if x in {'no', 'none'} else pd.NA))
        df['multiple lines'] = df['multiple lines'].map(lambda x: 1 if x in {'yes', 'multiple'} else (0 if x in {'no', 'none'} else pd.NA))
        df['internet service'] = df['internet service'].map(lambda x: 1 if x == 'yes' else (0 if x in {'no', 'none'} else pd.NA))
        df['online security'] = df['online security'].map(lambda x: 1 if x in {'yes', 'enabled'} else (0 if x in {'no', 'none'} else pd.NA))
        df['online backup'] = df['online backup'].map(lambda x: 1 if x in {'yes', 'enabled'} else (0 if x in {'no', 'none'} else pd.NA))

        simple_bool_cols = ['device protection plan', 'premium tech support', 'streaming tv', 'streaming movies', 'streaming music', 'unlimited data', 'referred a friend']
        for col in simple_bool_cols:
            df[col] = df[col].map(lambda x: 1 if x == 'yes' else (0 if x == 'no' else pd.NA))

        # Clean categorial column
        df['internet type'] = df['internet type'].map(lambda x: x if x in {'dsl', 'fiber optic'} else ('none' if x == '' else pd.NA))

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=df_fields)
        df['number of referrals'] = df['number of referrals'].astype(int)
        df['avg monthly long distance charges'] = df['avg monthly long distance charges'].astype(float)
        df['avg monthly gb download'] = df['avg monthly gb download'].astype(int)

        logging.info(f"Services data transformed: {len(df)}/{original_rows} rows retained")

        # Save cleaned file
        df = df.rename(columns={'customer id': 'customer_id', 'referred a friend': 'has_referred_a_friend', 'number of referrals': 'number_of_referrals', 'phone service': 'has_phone_service', 'multiple lines': 'has_multiple_lines', 'internet service': 'has_internet_service', 'internet type': 'internet_service_type', 'online security': 'has_online_security', 'online backup': 'has_online_backup', 'avg monthly long distance charges': 'avg_monthly_long_distance_charges', 'avg monthly gb download': 'avg_monthly_gb_download', 'device protection plan': 'has_device_protection', 'premium tech support': 'has_tech_support', 'streaming tv': 'has_tv', 'streaming movies': 'has_movies', 'streaming music': 'has_music', 'unlimited data': 'has_unlimited_data'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'services_cleaned.csv'
        df[['customer_id', 'has_referred_a_friend', 'number_of_referrals', 'has_phone_service', 'has_multiple_lines', 'has_internet_service', 'internet_service_type', 'has_online_security', 'has_online_backup', 'avg_monthly_long_distance_charges', 'avg_monthly_gb_download', 'has_device_protection', 'has_tech_support', 'has_tv', 'has_movies', 'has_music', 'has_unlimited_data']].to_csv(cleaned_path, index=False)


    @task
    def transform_financials_data(file_path: str) -> str:
        """Transform financials data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned customer id column!")

        # Clean Boolean column
        df['paperless billing'] = df['paperless billing'].map(lambda x: 1 if x in {'yes', 'electronic', 'digital'} else (0 if x in {'no', 'paper', 'mail'} else pd.NA))

        # Clean numeric columns
        df['total charges'] = df['total charges'].str.strip("$").str.replace(',', '')
        
        numeric_cols = ['monthly charge', 'total charges', 'total refunds', 'total extra data charges', 'total long distance charges', 'total revenue']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['customer id', 'contract', 'paperless billing', 'payment method'] + numeric_cols)
        
        for col in numeric_cols:
            df[col] = df[col].astype(float)


        logging.info(f"Financials data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'customer id': 'customer_id', 'contract': 'contract_type', 'paperless billing': 'has_paperless_billing', 'payment method': 'payment_method', 'monthly charge': 'monthly_charges', 'total charges': 'total_charges', 'total refunds': 'total_refunds', 'total extra data charges': 'total_extra_data_charges', 'total long distance charges': 'total_long_distance_charges', 'total revenue': 'total_revenue'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'financials_cleaned.csv'
        df[['customer_id', 'contract_type', 'has_paperless_billing', 'payment_method', 'monthly_charges', 'total_charges', 'total_refunds', 'total_extra_data_charges', 'total_long_distance_charges', 'total_revenue']].to_csv(cleaned_path, index=False)


    @task
    def transform_status_data(file_path: str) -> str:
        """Transform account status data"""
        df = pd.read_csv(file_path)
        original_rows = len(df)

        df = clean_Customer_ID_col(df)
        logging.info("Cleaned customer id column!")

        # Clean numerical columns
        num_cols = ['cltv', 'satisfaction score', 'churn score']
        for col in num_cols:
            df[col] = df[col].str.strip('$').str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean categorial column
        df['churn category'] = df['churn category'].map(lambda x: x if x in {'competitor', 'dissatisfaction', 'attitude', 'price', 'other'} else ('not_specified' if x == '' else pd.NA))

        # Drop null or invalid rows
        df = df.replace('', pd.NA)
        df = df.dropna(subset=['customer id', 'satisfaction score', 'churn value', 'churn score', 'cltv', 'churn category'])

        logging.info(f"Status data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        df = df.rename(columns={'customer id': 'customer_id', 'satisfaction score': 'satisfaction_score', 'churn value': 'churn_label', 'churn score': 'churn_score', 'churn category': 'churn_category'})
        cleaned_path = Path(CONFIG['processed_data_path']) / 'status_cleaned.csv'
        df[['customer_id', 'satisfaction_score', 'churn_label', 'churn_score', 'cltv', 'churn_category']].to_csv(cleaned_path, index=False)


    @task
    def load_to_database(table_name: str, file_path: str):
        """Load cleaned data to MySQL database"""

        hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
        cnx = hook.get_conn()
        engine = create_engine(hook.get_uri(), creator=lambda: cnx)
       
        df = pd.read_csv(file_path)
        
        with engine.connect() as conn:
            conn.execute("SET FOREIGN_KEY_CHECKS=0;")
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            # Add unique index for foreign key support if needed
            if table_name == 'customer_profile':
                conn.execute("ALTER TABLE customer_profile ADD UNIQUE (customer_id);")
            if table_name == 'population':
                conn.execute("ALTER TABLE population ADD UNIQUE (zip_code);")
            conn.execute("SET FOREIGN_KEY_CHECKS=1;")

        engine.dispose()

        logging.info(f"Successfully loaded {len(df)} {table_name} records")
    
    # Extract all sources into dataframes
    for source in DATA_SOURCES.keys():
        validate_source(source)
    
    # Transform dataframes (dependency is implicit here since we are using the output of one task as input to another)

    transform_customer_profile_data(DATA_SOURCES['customer_profile']['file_path'])
    transform_demographics_data(DATA_SOURCES['demographics']['file_path'])

    transform_location_data(DATA_SOURCES['location']['file_path'])
    transform_population_data(DATA_SOURCES['population']['file_path'])

    transform_services_data(DATA_SOURCES['services']['file_path'])
    transform_financials_data(DATA_SOURCES['services']['file_path'])

    transform_status_data(DATA_SOURCES['status']['file_path'])

    # Load dataframes into database
    load_customer_profile = load_to_database('customer_profile', Path(CONFIG['processed_data_path']) / 'churn_cleaned.csv')     # everything is dependent on customer profile data

    # Load population and location with explicit dependency
    load_population = load_to_database('population', Path(CONFIG['processed_data_path']) / 'population_cleaned.csv')
    load_location = load_to_database('location', Path(CONFIG['processed_data_path']) / 'location_cleaned.csv')
    load_customer_profile >> load_population >> load_location
    
    # Load remaining sources while keeping dependencies in mind
    load_tasks = []
    for name in ['demographics', 'services', 'financials', 'status']:
        load_task = load_to_database(name, Path(CONFIG['processed_data_path']) / f'{name}_cleaned.csv')
        load_tasks.append(load_task)
        # customer_profile must load before all other tables
        load_customer_profile >> load_task

    

# Instantiate the DAG
telco_etl()