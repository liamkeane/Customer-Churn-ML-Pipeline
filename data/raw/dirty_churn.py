import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import string
import warnings

def dirty_telco_data(df, dirty_percentage=0.10):
    """
    Introduce realistic data quality issues into IBM's telco churn dataset
    
    Args:
        df: Clean pandas DataFrame
        dirty_percentage: Proportion of records to affect (0.0 to 1.0)
    
    Returns:
        Dirtied DataFrame
    """
    
    # Make a copy to avoid modifying original
    dirty_df = df.copy()
    n_rows = len(dirty_df)
    
    # 1. Missing Values (NULL, empty strings, various representations)
    missing_representations = ['', 'NULL', 'null', 'N/A', 'na', 'missing', '?', '-', 'unknown']
    
    # Introduce missing values in different columns
    for col in ['Senior Citizen', 'Partner', 'Dependents', 'Phone Service', 'Internet Service']:
        n_missing = int(n_rows * dirty_percentage * 0.3)  # 30% of dirty records get missing values
        missing_indices = random.sample(range(n_rows), n_missing)
        for i in missing_indices:
            dirty_df.loc[i, col] = random.choice(missing_representations)
    
    # 2. Inconsistent Categorical Values
    categorical_mappings = {
        'Senior Citizen': {'No': ['no', 'NO', 'n', 'N', 'false', 'False', '0'], 
                          'Yes': ['yes', 'YES', 'y', 'Y', 'true', 'True', '1']},
        'Partner': {'No': ['no', 'NO', 'single', 'Single', 'unmarried'], 
                   'Yes': ['yes', 'YES', 'married', 'Married', 'partnered']},
        'Dependents': {'No': ['no', 'NO', 'none', 'None', '0'], 
                      'Yes': ['yes', 'YES', 'children', 'kids', 'family']},
        'Phone Service': {'No': ['no', 'NO', 'none', 'None'], 
                         'Yes': ['yes', 'YES', 'active', 'enabled']},
        'Paperless Billing': {'No': ['no', 'NO', 'paper', 'mail'], 
                             'Yes': ['yes', 'YES', 'electronic', 'digital']},
        'Churn': {'No': ['no', 'NO', 'stayed', 'retained', 'active'], 
                 'Yes': ['yes', 'YES', 'left', 'churned', 'inactive']}
    }
    
    for col, mappings in categorical_mappings.items():
        n_inconsistent = int(n_rows * dirty_percentage * 0.4)
        inconsistent_indices = random.sample(range(n_rows), n_inconsistent)
        for idx in inconsistent_indices:
            current_val = dirty_df.loc[idx, col]
            if current_val in mappings:
                dirty_df.loc[idx, col] = random.choice(mappings[current_val])
    
    # 3. Data Type Issues
    # Convert some numeric columns to strings with formatting issues
    numeric_cols = ['Tenure', 'Monthly Charges', 'Total Charges']
    for col in numeric_cols:
        n_format_issues = int(n_rows * dirty_percentage * 0.2)
        format_indices = random.sample(range(n_rows), n_format_issues)
        for idx in format_indices:
            val = dirty_df.loc[idx, col]
            # Add various formatting issues
            format_type = random.choice(['currency', 'spaces', 'commas', 'text'])
            if format_type == 'currency':
                dirty_df.loc[idx, col] = f"${val}"
            elif format_type == 'spaces':
                dirty_df.loc[idx, col] = f" {val} "
            elif format_type == 'commas' and float(val) > 1000:
                dirty_df.loc[idx, col] = f"{float(val):,.2f}"
            elif format_type == 'text':
                dirty_df.loc[idx, col] = f"{val} months" if col == 'Tenure' else f"{val} dollars"
    
    # 4. Duplicate Records
    n_duplicates = int(n_rows * dirty_percentage * 0.1)
    for _ in range(n_duplicates):
        # Pick a random row to duplicate
        original_idx = random.randint(0, len(dirty_df) - 1)
        duplicate_row = dirty_df.iloc[original_idx].copy()
        
        # Append duplicate
        dirty_df = pd.concat([dirty_df, duplicate_row.to_frame().T], ignore_index=True)
    
    
    # 6. Inconsistent Spacing and Case
    text_cols = [col for col in dirty_df.columns if dirty_df[col].dtype == 'object']
    for col in text_cols:
        n_spacing = int(n_rows * dirty_percentage * 0.2)
        spacing_indices = random.sample(range(len(dirty_df)), min(n_spacing, len(dirty_df)))
        for idx in spacing_indices:
            val = str(dirty_df.loc[idx, col])
            spacing_type = random.choice(['leading_space', 'trailing_space', 'mixed_case', 'all_caps'])
            if spacing_type == 'leading_space':
                dirty_df.loc[idx, col] = '  ' + val
            elif spacing_type == 'trailing_space':
                dirty_df.loc[idx, col] = val + '  '
            elif spacing_type == 'mixed_case':
                dirty_df.loc[idx, col] = ''.join(c.upper() if i % 2 else c.lower() for i, c in enumerate(val))
            elif spacing_type == 'all_caps':
                dirty_df.loc[idx, col] = val.upper()
    
    # 7. Add some completely invalid rows
    n_invalid = int(n_rows * dirty_percentage * 0.05)
    for _ in range(n_invalid):
        invalid_row = dirty_df.iloc[0].copy()  # Use first row as template
        # Make most fields invalid
        for col in text_cols:
            if random.random() < 0.7:  # 70% chance to corrupt each field
                invalid_row[col] = random.choice(['###ERROR###', 'CORRUPT_DATA', '<<<INVALID>>>', 'DATA_LOSS', 'AHHHHH', 'PANIC!'])
        dirty_df = pd.concat([dirty_df, invalid_row.to_frame().T], ignore_index=True)
    
    return dirty_df

def create_data_quality_report(original_df, dirty_df):
    """Generate a report showing what was dirtied"""
    print("=== DATA QUALITY ISSUES INTRODUCED ===\n")
    
    print(f"Original rows: {len(original_df)}")
    print(f"Dirty rows: {len(dirty_df)}")
    print(f"Added rows (duplicates + invalid): {len(dirty_df) - len(original_df)}\n")
    
    # Check missing values
    print("Missing values per column:")
    for col in dirty_df.columns:
        missing_count = dirty_df[col].isin(['', 'NULL', 'null', 'N/A', 'na', 'missing', '?', '-', 'unknown']).sum()
        if missing_count > 0:
            print(f"  {col}: {missing_count} missing values")
    
    # Check data type issues
    print(f"\nData type inconsistencies:")
    for col in ['Tenure', 'Monthly Charges', 'Total Charges']:
        if col in dirty_df.columns:
            non_numeric = pd.to_numeric(dirty_df[col], errors='coerce').isna().sum()
            print(f"  {col}: {non_numeric} non-numeric values")
    
    print(f"\nSample of dirtied data:")
    print(dirty_df.head(10))


if __name__ == "__main__":

    df = pd.read_csv('churn.csv')
    # print(df.info())
    
    # Dirty the data
    dirty_df = dirty_telco_data(df, dirty_percentage=0.10)
    
    # Generate report
    create_data_quality_report(df, dirty_df)
    
    # Save to CSV
    dirty_df.to_csv('churn_dirty.csv', index=False)
    print(f"\nDirty dataset saved as 'churn_dirty.csv'")