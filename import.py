#!/usr/bin/env python3
import pytds
import csv
import glob
import os
import json
import logging
import shutil
import hashlib
import zipfile

# Database connection parameters
DRIVER = "ODBC Driver 17 for SQL Server"
DB_HOST = "localhost"
DB_USER = "sa"
DB_PASS = "Abcd@1234"
DB_NAME = "AggregateDB"

# Configure logging
logging.basicConfig(
    filename='import_tool.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load configuration file for report name mappings
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file {config_path} not found.")
        return {}
    except json.JSONDecodeError:
        logging.error("Error decoding JSON configuration file.")
        return {}


def calculate_checksum(file_path):
    """Calculates the SHA-256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def verify_and_extract_zip_files(directory):
    """Verifies all zip files in the directory against their checksum files."""
    zip_files = glob.glob(os.path.join(directory, "*.zip"))
    
    if not zip_files:
        print("No zip files found in the directory.")
        return
    
    for zip_path in zip_files:
        # Derive the corresponding checksum file path
        checksum_path = f"{zip_path}_checksum.txt".replace(".zip","")
        
        if not os.path.exists(checksum_path):
            print(f"No checksum file found for: {zip_path}")
            continue
        
        # Calculate the actual checksum of the zip file
        actual_checksum = calculate_checksum(zip_path)
        
        # Read the expected checksum from the checksum file
        with open(checksum_path, "r") as checksum_file:
            expected_checksum = checksum_file.read().strip()
        
        # Compare checksums
        if actual_checksum == expected_checksum:
            logging.info(f"{os.path.basename(zip_path)}: Checksum verified successfully.")
            extract_zip_without_directory(zip_path, directory)
        else:
            logging.error(f"{os.path.basename(zip_path)}: Checksum verification failed.")


def extract_zip_without_directory(zip_path, target_directory):
    """
    Extracts all files from the zip file at zip_path into target_directory
    without preserving directory structure.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            # Check if it is a file (not a directory)
            if not member.endswith('/'):
                # Define the full path for the extracted file
                extracted_path = os.path.join(target_directory, os.path.basename(member))
                
                # Extract the file content to the specified path
                with open(extracted_path, 'wb') as output_file:
                    output_file.write(zip_ref.read(member))
# Read CSV files based on report_name mappings from config and include month/year
def read_csv_files_based_on_config(directory, config):
    file_pattern = os.path.join(directory, "*.csv")
    matching_files = glob.glob(file_pattern)
    
    if not matching_files:
        logging.error("No CSV files found in the directory.")
        return None
    
    csv_data_by_report = {}
    
    for csv_file_path in matching_files:
        filename = os.path.basename(csv_file_path).replace(".csv", "")
        parts = filename.split("_")
        if len(parts) < 3:
            logging.error(f"Skipping file with unexpected format: {filename}")
            continue
        
        report_name = "_".join(parts[:-3])  # Extract report name without month/year
        month = parts[-2]  # Extract the month part
        year = parts[-1]  # Extract the year part
        
        # Check if the report name is in the config file
        if report_name in config:
            # Read the CSV file
            data = []
            with open(csv_file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)  # The first row is the header
                data.append(headers)
                for row in reader:
                    data.append(row)
            
            csv_data_by_report[report_name] = {
                "description": config[report_name].get("description", ""),
                "month": month,
                "year": year,
                "data": data,
                "header_mapping": config[report_name].get("header_mapping", {})
                # Map CSV headers to DB columns
            }
        else:
            logging.error(f"Report name '{report_name}' not found in the configuration file.")
    
    return csv_data_by_report

def generate_insert_query(table_name, header_mapping):
    columns = ', '.join(header_mapping.values())
    placeholders = ', '.join(['%({})s'.format(col) for col in header_mapping.values()])
    generated_insert_query = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders});
    """
    return generated_insert_query


def process_data_and_insert(cursor, report_data,report):
    table_name = report
    header_mapping = report_data.get("header_mapping", {})
    data = report_data["data"]
    year = report_data["year"]
    month = report_data["month"]
    
    # Skip header row in data
    for row in data[1:]:
        # Dynamically map CSV row to database columns using the header mapping
        values = {header_mapping.get(header, header): row[idx] for idx, header in
                  enumerate(data[0])}
        values["ReportYear"] = year
        values["ReportMonth"] = month
        
        insert_query = generate_insert_query(table_name, header_mapping)
        cursor.execute(insert_query, values)
        
def move_imported_file(file_pattern):
    os.makedirs('processed_csv', exist_ok=True)
    for file_path in glob.glob(file_pattern):
        try:
            # Construct the target path in 'processed_csv'
            target_path = os.path.join('processed_csv', os.path.basename(file_path))
            # Replace the file if it already exists
            if os.path.exists(target_path):
                os.replace(file_path, target_path)
                logging.info(f"Replaced existing file: {target_path}")
            else:
                shutil.move(file_path, target_path)
                logging.info(f"Moved file: {file_path} to {target_path}")
        except OSError as e:
            logging.error(f"Error moving file {file_path}: {e}")
    return {}

    
# Usage
verify_and_extract_zip_files('output_csv')

# Main execution block
with pytds.connect(DB_HOST, DB_NAME, DB_USER, DB_PASS) as conn:
    cursor = conn.cursor()
    
    # Load configuration
    config_path = "import_config.json"
    config = load_config(config_path)
    
    # Specify the directory containing CSV files
    directory = "output_csv"
    
    # Read CSV files based on the config
    csv_data = read_csv_files_based_on_config(directory, config)
    
    if csv_data:
        for report, content in csv_data.items():
            logging.info(
            f"\nData for {report} ({content['description']}, Month: {content['month']}, Year: {content['year']}):")
            HMIS_CODE = content['data'][2][-1]
            delete_existing = """
                DELETE FROM  {}  WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'
                """.format(report,HMIS_CODE, content['year'], content['month'])
            cursor.execute(delete_existing)
            # Process data and insert dynamically
            process_data_and_insert(cursor, content,report)
            
            file_pattern = os.path.join('output_csv',
                                        f"*{content['data'][2][-2]}{content['data'][2][-1]}_{content['month']}_{content['year']}.csv")
            move_imported_file(file_pattern)
            sp_query = f"""
            EXEC SP_AggregateHivindicators '{content['data'][2][-4]}', '{content['data'][2][-3]}', '{content['data'][2][-2]}', '{content['data'][2][-1]}', '{content['year']}', '{content['month']}'
            """
            cursor.execute(sp_query)
    # Commit the transaction
    conn.commit()
    cursor.close()
    
