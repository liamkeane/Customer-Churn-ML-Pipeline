from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import logging
from typing import Dict, List, Any
from pathlib import Path

# Configuration for all data sources
DATA_SOURCES = {
    'churn': {
        'file_path': '/opt/airflow/data/interim/telco_churn_dirty.csv',
        'table': 'customer_churn',
        'primary_key': 'customer_id',
        'depends_on': ['demographics', 'services']
    },
    'demographics': {
        'file_path': '/opt/airflow/data/interim/customer_demographics_dirty.csv',
        'table': 'customer_demographics', 
        'primary_key': 'customer_id',
        'depends_on': []
    },
    'location': {
        'file_path': '/opt/airflow/data/interim/customer_location_dirty.csv',
        'table': 'customer_location',
        'primary_key': 'customer_id',
        'depends_on': ['population']
    },
    'population': {
        'file_path': '/opt/airflow/data/interim/area_population_dirty.csv',
        'table': 'area_population',
        'primary_key': 'area_code',
        'depends_on': []
    },
    'services': {
        'file_path': '/opt/airflow/data/interim/customer_services_dirty.csv',
        'table': 'customer_services',
        'primary_key': 'customer_id',
        'depends_on': []
    },
    'status': {
        'file_path': '/opt/airflow/data/interim/account_status_dirty.csv',
        'table': 'account_status',
        'primary_key': 'customer_id',
        'depends_on': ['churn']
    }
}

CONFIG = {
    'mysql_conn_id': 'mysql_telco_conn',
    'processed_data_path': '/opt/airflow/data/processed/',
    'batch_size': 1000
}

def clean_customer_id_col(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize columns
    for col in df.columns:
        df[col] = df[col].strip().lower()

    df = df[df['customer_id'].str.match(r'^\d{4}-[A-Z]{5}$', na=False)]     # must match this *exact* pattern
    df = df.drop_duplicates(subset=['customer_id'])    # gotta be unique since its our primary key
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

        df = clean_customer_id_col(df)

        # Clean tenure field
        df['Tenure'] = df['Tenure'].str.strip().str.strip("$").str.replace(' months', '')
        df['tenure_months'] = pd.to_numeric(df['Tenure'], errors='coerce')

        # Drop null or invalid rows
        df = df[(df['customer_id'] != '') & (df['Tenure'] != '')]
        df = df.dropna(subset=['customer_id', 'Tenure'])
        
        # Save cleaned file
        cleaned_path = Path(CONFIG['processed_data_path']) / 'churn_cleaned.csv'
        df[['customer_id', 'tenure_months']].to_csv(cleaned_path, index=False)

        logging.info(f"Churn data transformed: {len(df)}/{original_rows} rows retained")

        # Return data frame
        return df[['customer_id', 'tenure_months']]

    @task
    def transform_demographics_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform demographics data"""
        original_rows = len(df)

        df = clean_customer_id_col(df)

        # Clean Age column
        df['Age'] = df['Age'].str.strip("$")
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')

        # Clean categorical column
        df = df[df['Gender'].isin(['male', 'female'])]

        # Clean Boolean columns
        df = df[df['Under 30'].isin(['yes', 'no'])]
        df = df[df['Senior Citizen'].isin(['yes', 'no'])]
        df = df[df['Married'].isin(['yes', 'no', 'partnered', 'married'])]
        df = df[df['Dependents'].isin(['yes', 'no'])]
        
        # Drop null or invalid rows
        df = df[(df['customer_id'] != '') & (df['Tenure'] != '')]
        df = df.dropna(subset=['customer_id', 'Tenure'])
        
        cleaned_path = Path(CONFIG['processed_data_path']) / 'demographics_cleaned.csv'
        df[['customer_id', '']].to_csv(cleaned_path, index=False)
        
        logging.info(f"Demographics data transformed: {len(df)}/{original_rows} rows retained")
        return str(cleaned_path)
    
    @task 
    def transform_location_data(extraction_result: Dict[str, Any]) -> str:
        """Transform location data"""
        if extraction_result['status'] != 'PASS':
            raise ValueError(f"Location data failed validation: {extraction_result}")
        
        df = pd.read_csv(extraction_result['raw_file_path'])
        original_rows = len(df)
        
        # Location-specific transformations
        # Standardize state abbreviations
        if 'state' in df.columns:
            df['state'] = df['state'].str.upper().str.strip()
        
        # Clean zip codes
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str.extract(r'(\d{5})')
        
        # Standardize city names
        if 'city' in df.columns:
            df['city'] = df['city'].str.title().str.strip()
        
        cleaned_path = Path(CONFIG['processed_data_path']) / 'location_cleaned.csv'
        df.to_csv(cleaned_path, index=False)
        
        logging.info(f"Location data transformed: {len(df)}/{original_rows} rows retained")
        return str(cleaned_path)
    
    @task
    def transform_generic_data(source_name: str, extraction_result: Dict[str, Any]) -> str:
        """Generic transformation for population, services, and status data"""
        if extraction_result['status'] != 'PASS':
            raise ValueError(f"{source_name} data failed validation: {extraction_result}")
        
        df = pd.read_csv(extraction_result['raw_file_path'])
        original_rows = len(df)
        
        # Generic cleaning
        # Remove rows with all null values
        df = df.dropna(how='all')
        
        # Clean string columns
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['NULL', 'null', 'N/A', 'na', ''], pd.NA)
        
        # Clean numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        cleaned_path = Path(CONFIG['processed_data_path']) / f'{source_name}_cleaned.csv'
        df.to_csv(cleaned_path, index=False)
        
        logging.info(f"{source_name} data transformed: {len(df)}/{original_rows} rows retained")
        return str(cleaned_path)
    
    @task
    def load_to_database(source_name: str, cleaned_file_path: str) -> Dict[str, Any]:
        """Load cleaned data to MySQL database"""
        config = DATA_SOURCES[source_name]
        table_name = config['table']
        
        df = pd.read_csv(cleaned_file_path)
        mysql_hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
        
        try:
            # Batch insert
            batch_size = CONFIG['batch_size']
            total_inserted = 0
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                # Convert to records
                records = []
                for _, row in batch_df.iterrows():
                    record = tuple(row[col] if pd.notna(row[col]) else None for col in batch_df.columns)
                    records.append(record)
                
                # Insert batch
                mysql_hook.insert_rows(
                    table=table_name,
                    rows=records,
                    target_fields=list(batch_df.columns),
                    replace=True  # Use REPLACE for upsert behavior
                )
                
                total_inserted += len(records)
                logging.info(f"{source_name}: Inserted batch {i//batch_size + 1} ({len(records)} records)")
            
            result = {
                'source': source_name,
                'table': table_name,
                'records_loaded': total_inserted,
                'status': 'SUCCESS'
            }
            
            logging.info(f"Successfully loaded {total_inserted} {source_name} records")
            return result
            
        except Exception as e:
            logging.error(f"Failed to load {source_name} data: {str(e)}")
            return {
                'source': source_name,
                'table': table_name,
                'status': 'FAILED',
                'error': str(e)
            }
    
    @task
    def validate_referential_integrity() -> Dict[str, Any]:
        """Validate foreign key relationships across tables"""
        mysql_hook = MySqlHook(mysql_conn_id=CONFIG['mysql_conn_id'])
        integrity_issues = {}
        
        # Define expected relationships
        relationships = [
            ('customer_churn', 'customer_id', 'customer_demographics', 'customer_id'),
            ('customer_churn', 'customer_id', 'customer_services', 'customer_id'),
            ('customer_location', 'customer_id', 'customer_demographics', 'customer_id'),
            ('customer_location', 'area_code', 'area_population', 'area_code')
        ]
        
        for parent_table, parent_key, child_table, child_key in relationships:
            try:
                # Check for orphaned records
                orphan_query = f"""
                SELECT COUNT(*) as orphan_count
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_key} = p.{parent_key}
                WHERE p.{parent_key} IS NULL AND c.{child_key} IS NOT NULL
                """
                
                result = mysql_hook.get_first(orphan_query)
                orphan_count = result[0] if result else 0
                
                integrity_issues[f"{child_table}_{child_key}"] = {
                    'parent_table': parent_table,
                    'orphan_count': orphan_count,
                    'status': 'PASS' if orphan_count == 0 else 'WARN'
                }
                
            except Exception as e:
                integrity_issues[f"{child_table}_{child_key}"] = {
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        overall_status = 'PASS'
        if any(issue['status'] == 'ERROR' for issue in integrity_issues.values()):
            overall_status = 'FAIL'
        elif any(issue['status'] == 'WARN' for issue in integrity_issues.values()):
            overall_status = 'WARN'
        
        return {
            'overall_status': overall_status,
            'integrity_issues': integrity_issues,
            'validation_timestamp': datetime.now().isoformat()
        }
    
    @task
    def generate_pipeline_summary(load_results: List[Dict], integrity_result: Dict) -> str:
        """Generate comprehensive pipeline execution summary"""
        
        total_records = sum(result.get('records_loaded', 0) for result in load_results)
        successful_loads = sum(1 for result in load_results if result.get('status') == 'SUCCESS')
        failed_loads = len(load_results) - successful_loads
        
        summary = f"""
        Multi-Source ETL Pipeline Summary
        ================================
        
        OVERALL STATUS: {'SUCCESS' if failed_loads == 0 else 'PARTIAL SUCCESS' if successful_loads > 0 else 'FAILED'}
        
        DATA SOURCES PROCESSED: {len(DATA_SOURCES)}
        • Successful Loads: {successful_loads}
        • Failed Loads: {failed_loads}
        • Total Records Loaded: {total_records:,}
        
        LOAD RESULTS BY SOURCE:
        """
        
        for result in load_results:
            status_icon = "✓" if result.get('status') == 'SUCCESS' else "✗"
            records = result.get('records_loaded', 0)
            summary += f"\n  {status_icon} {result.get('source', 'Unknown')}: {records:,} records"
        
        summary += f"""
        
        REFERENTIAL INTEGRITY: {integrity_result.get('overall_status', 'UNKNOWN')}
        """
        
        for key, issue in integrity_result.get('integrity_issues', {}).items():
            if issue.get('orphan_count', 0) > 0:
                summary += f"\n  ⚠ {key}: {issue['orphan_count']} orphaned records"
        
        summary += f"""
        
        Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        # Save summary
        summary_path = Path(CONFIG['processed_data_path']) / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        summary_path.write_text(summary)
        
        logging.info("Pipeline summary generated")
        return summary
    
    
    # Extract all sources
    extracted_dataframes = {}
    for source in DATA_SOURCES.keys():
        extracted_dataframes[source] = extract_and_validate_source(source)
    
    # Transform based on dependencies and source type
    transformed_dataframes = {}

    transformed_dataframes['churn'] = transform_customer_profile_data(extracted_dataframes['churn'])

    # Demographics and population have no dependencies
    transformed_dataframes['demographics'] = transform_demographics_data(extracted_dataframes['demographics'])
    transformed_dataframes['population'] = transform_generic_data('population', extracted_dataframes['population'])
    
    # Services can run after demographics
    transformed_dataframes['services'] = transform_generic_data('services', extracted_dataframes['services'])
    
    # Location depends on population  
    transformed_dataframes['location'] = transform_location_data(extracted_dataframes['location'])
    transformed_dataframes['location'] >> transformed_dataframes['population']  # dependency
    
    # Churn depends on demographics and services
    
    [transformed_dataframes['demographics'], transformed_dataframes['services']] >> transformed_dataframes['churn']
    
    # Status depends on churn
    transformed_dataframes['status'] = transform_generic_data('status', extracted_dataframes['status'])
    transformed_dataframes['churn'] >> transformed_dataframes['status']
    
    # Load all sources
    load_results = []
    for source in DATA_SOURCES.keys():
        load_task = load_to_database(source, transformed_dataframes[source])
        load_results.append(load_task)
    
    # Final validation and summary
    integrity_check = validate_referential_integrity()
    load_results >> integrity_check
    
    pipeline_summary = generate_pipeline_summary(load_results, integrity_check)
    
    return pipeline_summary

# Instantiate the DAG
dag_instance = telco_etl()