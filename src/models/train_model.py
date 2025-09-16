import json
import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

class TelcoChurnModel:
    def __init__(self, db_config):
        """
        Initialize the model with database configuration
        
        db_config: dict with keys 'host', 'port', 'user', 'password', 'database'
        """
        self.db_config = db_config
        self.engine = None
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        
    def connect_to_database(self):
        """Create SQLAlchemy engine for database connection"""
        try:
            connection_string = f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            print("Successfully connected to MySQL database")
            return True
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def load_data(self, query):
        """
        Load data from MySQL database
        """
        if not self.engine:
            if not self.connect_to_database():
                return None
            
        try:
            df = pd.read_sql(query, self.engine)
            print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    def preprocess_data(self, df, target_column='Churn'):
        """
        Preprocess the telco churn dataset
        """
        # Make a copy to avoid modifying original data
        data = df.copy()
        
        # Handle missing values
        data = data.dropna()
        
        # Convert TotalCharges to numeric (it's often stored as string)
        if 'TotalCharges' in data.columns:
            data['TotalCharges'] = pd.to_numeric(data['TotalCharges'], errors='coerce')
            data = data.dropna()  # Drop rows where TotalCharges couldn't be converted
        
        # Separate features and target
        if target_column not in data.columns:
            raise ValueError(f"Target column '{target_column}' not found in data")
        
        X = data.drop(target_column, axis=1)
        y = data[target_column]
        
        # Handle target variable (convert Yes/No to 1/0 if needed)
        if y.dtype == 'object':
            if target_column not in self.label_encoders:
                self.label_encoders[target_column] = LabelEncoder()
                y = self.label_encoders[target_column].fit_transform(y)
            else:
                y = self.label_encoders[target_column].transform(y)
        
        # Handle categorical variables
        categorical_columns = X.select_dtypes(include=['object']).columns
        
        for col in categorical_columns:
            if col not in self.label_encoders:
                self.label_encoders[col] = LabelEncoder()
                X[col] = self.label_encoders[col].fit_transform(X[col])
            else:
                X[col] = self.label_encoders[col].transform(X[col])
        
        # Scale numerical features
        X_scaled = self.scaler.fit_transform(X)
        X_scaled = pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
        
        print(f"Preprocessed data shape: {X_scaled.shape}")
        print(f"Target distribution: {pd.Series(y).value_counts()}")
        
        return X_scaled, y
    
    def train_model(self, X, y, test_size=0.2, random_state=42):
        """
        Train logistic regression model
        """
        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        
        # Initialize and train the model
        self.model = LogisticRegression(random_state=random_state, max_iter=1000)
        self.model.fit(X_train, y_train)
        
        # Make predictions
        y_train_pred = self.model.predict(X_train)
        y_test_pred = self.model.predict(X_test)
        
        # Calculate metrics
        train_accuracy = accuracy_score(y_train, y_train_pred)
        test_accuracy = accuracy_score(y_test, y_test_pred)
        
        print(f"Training Accuracy: {train_accuracy:.4f}")
        print(f"Testing Accuracy: {test_accuracy:.4f}")
        
        # Detailed classification report
        print("\nClassification Report (Test Set):")
        print(classification_report(y_test, y_test_pred))
        
        # Confusion Matrix
        print("\nConfusion Matrix (Test Set):")
        print(confusion_matrix(y_test, y_test_pred))
        
        return {
            'train_accuracy': train_accuracy,
            'test_accuracy': test_accuracy,
            'model': self.model,
            'X_test': X_test,
            'y_test': y_test,
            'y_pred': y_test_pred
        }
    
    def get_feature_importance(self, feature_names):
        """
        Get feature importance from the trained model
        """
        if not self.model:
            print("Model not trained yet!")
            return None
        
        # Get coefficients
        coefficients = self.model.coef_[0]
        
        # Create feature importance dataframe
        feature_importance = pd.DataFrame({
            'feature': feature_names,
            'coefficient': coefficients,
            'abs_coefficient': np.abs(coefficients)
        }).sort_values('abs_coefficient', ascending=False)
        
        print("Top 10 Most Important Features:")
        print(feature_importance.head(10))
        
        return feature_importance
    
    def predict_new_data(self, new_data):
        """
        Make predictions on new data
        """
        if not self.model:
            print("Model not trained yet!")
            return None
        
        # Preprocess new data (same steps as training data)
        # Note: This assumes new_data has the same structure as training data
        processed_data = new_data.copy()
        
        # Apply label encoders to categorical columns
        for col in processed_data.select_dtypes(include=['object']).columns:
            if col in self.label_encoders:
                processed_data[col] = self.label_encoders[col].transform(processed_data[col])
        
        # Scale the data
        processed_data_scaled = self.scaler.transform(processed_data)
        
        # Make predictions
        predictions = self.model.predict(processed_data_scaled)
        probabilities = self.model.predict_proba(processed_data_scaled)
        
        return predictions, probabilities
    
    def save_model(self, model_name='churn_model'):
        """
        Save the trained model and preprocessors using joblib
        """
        if not self.model:
            print("No model to save! Train the model first.")
            return False
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save model
            model_file = f"{self.model_path}{model_name}_{timestamp}.pkl"
            joblib.dump(self.model, model_file)
            
            # Save scaler
            scaler_file = f"{self.model_path}scaler_{model_name}_{timestamp}.pkl"
            joblib.dump(self.scaler, scaler_file)
            
            # Save label encoders
            encoders_file = f"{self.model_path}encoders_{model_name}_{timestamp}.pkl"
            joblib.dump(self.label_encoders, encoders_file)
            
            # Save metadata
            metadata = {
                'model_file': model_file,
                'scaler_file': scaler_file,
                'encoders_file': encoders_file,
                'timestamp': timestamp,
                'model_name': model_name
            }
            metadata_file = f"{self.model_path}metadata_{model_name}_{timestamp}.pkl"
            joblib.dump(metadata, metadata_file)
            
            print(f"Model saved successfully!")
            print(f"Model file: {model_file}")
            print(f"Use load_model() to load this model later")
            
            return metadata_file
            
        except Exception as e:
            print(f"Error saving model: {e}")
            return False
    
    def load_model(self, metadata_file=None, model_name='churn_model'):
        """
        Load a previously trained model and preprocessors using joblib
        """
        try:
            # If no specific file provided, find the latest model
            if not metadata_file:
                metadata_files = [f for f in os.listdir(self.model_path) 
                                if f.startswith(f'metadata_{model_name}') and f.endswith('.pkl')]
                
                if not metadata_files:
                    print(f"No saved models found with name '{model_name}'")
                    return False
                
                # Get the latest file (by timestamp)
                metadata_files.sort(reverse=True)
                metadata_file = os.path.join(self.model_path, metadata_files[0])
            
            # Load metadata
            metadata = joblib.load(metadata_file)
            
            # Load model components
            self.model = joblib.load(metadata['model_file'])
            self.scaler = joblib.load(metadata['scaler_file'])
            self.label_encoders = joblib.load(metadata['encoders_file'])
            
            print(f"Model loaded successfully from {metadata['timestamp']}")
            return True
            
        except Exception as e:
            print(f"Error loading model: {e}")
            return False
    
    def model_exists(self, model_name='churn_model'):
        """
        Check if a saved model exists
        """
        if not os.path.exists(self.model_path):
            return False
            
        metadata_files = [f for f in os.listdir(self.model_path) 
                         if f.startswith(f'metadata_{model_name}') and f.endswith('.pkl')]
        return len(metadata_files) > 0
    
    def list_saved_models(self):
        """
        List all saved models
        """
        if not os.path.exists(self.model_path):
            print("No models directory found")
            return []
            
        metadata_files = [f for f in os.listdir(self.model_path) 
                         if f.startswith('metadata_') and f.endswith('.pkl')]
        
        if not metadata_files:
            print("No saved models found")
            return []
        
        models_info = []
        for file in sorted(metadata_files, reverse=True):
            try:
                metadata = joblib.load(os.path.join(self.model_path, file))
                models_info.append({
                    'name': metadata['model_name'],
                    'timestamp': metadata['timestamp'],
                    'file': file
                })
            except:
                continue
        
        print("Saved models:")
        for i, info in enumerate(models_info):
            print(f"{i+1}. {info['name']} - {info['timestamp']}")
        
        return models_info

# Example usage
def main():
    # Database configuration for Docker MySQL
    with open('config.json') as f:
        db_config = json.load(f)
    
        # Initialize the model
        churn_model = TelcoChurnModel(db_config)
        
        # Check if a trained model already exists
        if churn_model.model_exists():
            print("Found existing trained model!")
            choice = input("Do you want to (1) Load existing model or (2) Train new model? Enter 1 or 2: ")
            
            if choice == '1':
                # Load existing model
                if churn_model.load_model():
                    print("Model loaded successfully! You can now make predictions.")
                    # Example: Load some data for prediction
                    df = churn_model.load_data(table_name='telco_churn')
                    if df is not None:
                        sample_data = df.drop('Churn', axis=1).head(5)  # Remove target column
                        predictions, probabilities = churn_model.predict_new_data(sample_data)
                        print(f"Sample predictions: {predictions}")
                    return
            else:
                print("Training new model...")
        
        # Train new model
        print("Loading data from database...")

        aggregation_query = '''select
                    c.customer_id,
                    c.tenure_months,
                    d.gender,
                    d.age,
                    d.is_under_30,
                    d.is_senior_citizen,
                    d.has_partner,
                    d.has_dependents,
                    d.number_of_dependents,
                    l.city,
                    l.zip_code,
                    p.population,
                    f.contract_type,
                    f.has_paperless_billing,
                    f.payment_method,
                    f.monthly_charges,
                    f.total_charges,
                    f.total_refunds,
                    f.total_extra_data_charges,
                    f.total_long_distance_charges,
                    f.total_revenue,
                    se.has_referred_a_friend,
                    se.number_of_referrals,
                    se.has_phone_service,
                    se.avg_monthly_long_distance_charges,
                    se.has_multiple_lines,
                    se.has_internet_service,
                    se.internet_service_type,
                    se.avg_monthly_gb_download,
                    se.has_online_security,
                    se.has_online_backup,
                    se.has_device_protection,
                    se.has_tech_support,
                    se.has_tv,
                    se.has_movies,
                    se.has_music,
                    se.has_unlimited_data,
                    st.satisfaction_score,
                    st.churn_label,
                    st.churn_score,
                    st.cltv,
                    st.churn_category
                from customer_profile c
                inner join demographics d on c.customer_id = d.customer_id
                inner join location l on c.customer_id = l.customer_id
                inner join population p on l.zip_code = p.zip_code
                inner join financials f on c.customer_id = f.customer_id
                inner join services se on c.customer_id = se.customer_id
                inner join status st on c.customer_id = st.customer_id;'''

        df = churn_model.load_data(query=aggregation_query)

        # Option for custom query (if you need to join tables, etc.)
        # custom_query = """
        # SELECT c.*, d.demographic_info 
        # FROM customers c 
        # LEFT JOIN demographics d ON c.customer_id = d.customer_id
        # """
        # df = churn_model.load_data(query=custom_query)
        
        if df is not None:
            # Preprocess the data
            print("Preprocessing data...")
            X, y = churn_model.preprocess_data(df, target_column='Churn')
            
            # Train the model
            print("Training model...")
            results = churn_model.train_model(X, y)
            
            # Get feature importance
            feature_importance = churn_model.get_feature_importance(X.columns)
            
            # Save the trained model
            print("\nSaving model...")
            metadata_file = churn_model.save_model()
            
            print("\nModel training completed successfully!")
            print("Next time you run this program, you can choose to load this trained model instead of retraining.")

def predict_only():
    """
    Function to only make predictions using a saved model
    """
    with open('config.json') as f:
        db_config = json.load(f)
    
        churn_model = TelcoChurnModel(db_config)
        
        # List available models
        churn_model.list_saved_models()
        
        # Load the latest model
        if churn_model.load_model():
            print("Model loaded! Ready for predictions.")
            
            # Load new data for prediction
            df = churn_model.load_data(table_name='new_customers')  # or whatever table
            if df is not None:
                # Remove target column if it exists
                if 'Churn' in df.columns:
                    df = df.drop('Churn', axis=1)
                
                predictions, probabilities = churn_model.predict_new_data(df)
                
                # Add predictions to dataframe
                df['Churn_Prediction'] = predictions
                df['Churn_Probability'] = probabilities[:, 1]  # Probability of churning
                
                print("Predictions completed!")
                print(df[['Churn_Prediction', 'Churn_Probability']].head())
        else:
            print("No trained model found. Please train a model first.")

if __name__ == "__main__":
    main()