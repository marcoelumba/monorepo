import sys
import pandas as pd
from sqlalchemy import text
import json
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os
from rapidfuzz import process

# Load environment variables from .env file
load_dotenv()

# Construct the database URL from environment variables
db_url = (
    f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_NAME')}"
)

# Define a dictionary mapping sheet URLs to table names
sheet_to_table_mapping = {
    "https://docs.google.com/spreadsheets/d/1-WIUFMWoAiNIMyVyNj4KKlk4Mrxkg0sIRdjarRqtrVs/edit?gid=666332733": "customer_engagement",
}

# Define the schema name
schema_name = "raw"

word_columns = ["Service", "Sub-Service", "Engagement Type", "Service Type", "Detailed Sub-Service"]



def load_public_google_sheet_to_dataframe(sheet_url: str) -> pd.DataFrame:
    """
    Load data from a public Google Spreadsheet into a Pandas DataFrame.

    Args:
        sheet_url (str): URL of the Google Spreadsheet.

    Returns:
        pd.DataFrame: Data loaded into a DataFrame.
    """
    # Convert the Google Sheets URL to its CSV export format
    csv_export_url = sheet_url.replace('/edit?gid=', '/export?format=csv&gid=')

    # Load the data into a DataFrame
    try:
        df = pd.read_csv(csv_export_url)
        return df
    except Exception as e:
        raise ValueError(f"Error loading data from the Google Sheet: {e}")

def save_dataframe_to_postgres(df: pd.DataFrame, db_url: str, schema: str, table_name: str):
    """
    Save a Pandas DataFrame to a PostgreSQL database table.

    Args:
        df (pd.DataFrame): DataFrame to save.
        db_url (str): SQLAlchemy database URL (e.g., postgresql://user:password@localhost/dbname).
        table_name (str): Name of the table to save the data.
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(db_url)

        # Save DataFrame to PostgreSQL
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)

        print(f"Data successfully saved to table '{table_name}' in the database.")
    except Exception as e:
        raise ValueError(f"Error saving DataFrame to PostgreSQL: {e}")
    
# Function to map misspelled words to correct words
def map_correct_word(column, correct_words):
    # Predefined mappings for specific exceptions
    exceptions = {"mew": "New", "cew": "New"}
    return column.apply(lambda x: exceptions[x] if x in exceptions else process.extractOne(x, correct_words)[0] if pd.notnull(x) else x)


def word_mapping(df: pd.DataFrame):
    # Correct words list
    correct_words = ["Design", "Support", "Consulting", "Development", "Backend", 
                     "Frontend" , "UX/UI", "Customer Service", "Strategy", "Recurring", 
                     "New", "One-time"]
    
    # Create a list to store mappings
    mappings = []
    
    # Iterate through each column that needs processing
    for col in word_columns: # ["Service", "Sub-Service", "Engagement Type", "Service Type", "Detailed Sub-Service"]:
        # Apply the mapping function
        df[col + "_Corrected"] = map_correct_word(df[col], correct_words)
        
        # Append mappings to the list for building the output DataFrame
        column_mappings = pd.DataFrame({
            "word": df[col],
            "correct_word": df[col + "_Corrected"],
            "update_date": pd.Timestamp.now().strftime("%Y-%m-%d")
        })
        mappings.append(column_mappings)
    
    # Combine all mappings into a single DataFrame
    final_mappings = pd.concat(mappings).drop_duplicates().reset_index(drop=True)
    save_dataframe_to_postgres(final_mappings, db_url, "common", "word_mapping")


# Loop through the dictionary
for sheet_url, table_name in sheet_to_table_mapping.items():
    try:
        print(f"Processing sheet: {sheet_url}")
        
        # Load data from Google Sheets
        dataframe = load_public_google_sheet_to_dataframe(sheet_url)
        
        # Save data to PostgreSQL within the specified schema
        save_dataframe_to_postgres(dataframe, db_url, schema_name, table_name)
        word_mapping(dataframe)

        print(f"Successfully processed and saved data to table '{schema_name}.{table_name}'")
    except Exception as e:
        print(f"Error processing sheet {sheet_url}: {e}")