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
import sys
from ethiopian_date import EthiopianDateConverter


config_file_name = "import_config.json"
data_directory = "exported_data"
conv = EthiopianDateConverter.to_gregorian
# Configure logging
logging.basicConfig(
    filename='import_tool.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
months = ["Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", "Yekatit", "Megabit", "Miyazia", "Ginbot",
          "Sene", "Hamle", "Nehase", "Puagume"]
month_mapping = {name: index + 1 for index, name in enumerate(months)}

def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):  # PyInstaller creates a temp folder and stores resources here
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

# Load configuration file for report name mappings
def load_config(config_file_name=config_file_name):
    try:
        with open(resource_path(config_file_name), 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error(f"Configuration file {resource_path(config_file_name)} not found.")
        return {}
    except json.JSONDecodeError:
        logging.error("Error decoding JSON configuration file.")
        return {}


# Load configuration
import_config = load_config(config_file_name)
# Database connection parameters
DRIVER = "ODBC Driver 17 for SQL Server"
DB_HOST = import_config["db_properties"]['DB_HOST']
DB_USER = import_config["db_properties"]['DB_USER']
DB_PASS = import_config["db_properties"]['DB_PASS']
DB_NAME = import_config["db_properties"]['DB_NAME']


# Load configuration file for report name mappings
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def verify_and_extract_zip_files(directory):
    zip_files = glob.glob(os.path.join(directory, "*.zip"))
    for index, zip_path in enumerate(zip_files, start=1):
        print_progress_bar(index, len(zip_files), prefix='Extracting Files', suffix='Complete', length=40)
        checksum_path = f"{zip_path}_checksum.txt".replace(".zip", "")
        if not os.path.exists(checksum_path):
            logging.error(f"No checksum file found for: {zip_path}")
            continue
        actual_checksum = calculate_checksum(zip_path)
        with open(checksum_path, "r") as checksum_file:
            expected_checksum = checksum_file.read().strip()
        if actual_checksum == expected_checksum:
            extract_zip_without_directory(zip_path, directory)
        else:
            logging.error(f"{os.path.basename(zip_path)}: Checksum verification failed.")


# Extract files from zip without directory structure
def extract_zip_without_directory(zip_path, target_directory):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            if not member.endswith('/'):
                extracted_path = os.path.join(target_directory, os.path.basename(member))
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
    
    for index, csv_file_path in enumerate(matching_files, start=1):
        total_files=len(matching_files)
        print_progress_bar(index+1, total_files, prefix='Reading CSV Files', suffix=f"{index}/{total_files}",
                           length=40)
        filename = os.path.basename(csv_file_path).replace(".csv", "")
        parts = filename.split("_")
        if len(parts) < 4:  # Ensure the file format has at least 4 parts
            logging.error(f"Skipping file with unexpected format: {filename}")
            continue
        
        facility = parts[-3]  # Extract the facility part
        report_name = "_".join(parts[:-3])  # Extract report name without facility/month/year
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
            
            # Use a composite key of report name, facility, month, and year
            report_key = f"{report_name}_{facility}_{month}_{year}"
            csv_data_by_report[report_key] = {
                "description": config[report_name].get("description", ""),
                "month": month,
                "year": year,
                "facility": facility,
                "data": data,
                "header_mapping": config[report_name].get("header_mapping", {}),
                "field_value_mapping": config[report_name].get("field_value_mapping", {})
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


def process_data_and_insert(cursor, report_data, report):
    table_name = report
    header_mapping = report_data.get("header_mapping", {})
    field_value_mapping = report_data.get("field_value_mapping", {})
    data = report_data["data"]
    year = report_data["year"]
    month = report_data["month"]
    
    # Skip header row in data
    for row in data[1:]:
        # Dynamically map CSV row to database columns using the header mapping
        values = {
            header_mapping.get(header, header): (row[idx])
            for idx, header in enumerate(data[0])
        }
        values["ReportYear"] = year
        values["ReportMonth"] = month
        for field, mappings in field_value_mapping.items():
            if field in values:
                original_value = values[field]
                # Check if the original value has a replacement in mappings
                if original_value in mappings:
                    values[field] = mappings[original_value]  # Replace value
        try:
            insert_query = generate_insert_query(table_name, header_mapping)
            cursor.execute(insert_query, values)
            # logging.info("Row %d: Inserted data into '%s'", values, table_name)
        except Exception as e:
            logging.error("Row %s: Failed to insert data into '%s': %s", values, table_name, e)
            continue


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


def print_progress_bar(iteration, total, prefix='', suffix='', length=50, fill='█', print_end="\r"):
    """
    Call in a loop to create terminal progress bar.
    """
    percent = f"{100 * (iteration / float(total)):.1f}"
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    # Print the progress bar and flush the output
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    sys.stdout.flush()  # Force the output to flush
    if iteration == total:
        print(f'\r{prefix} |{bar}| {percent}% Completed', end=print_end)
        print()  # Print a new line when complete

# Usage
verify_and_extract_zip_files(data_directory)
# Main execution block
sp_parameters = set()
try:
    with pytds.connect(DB_HOST, DB_NAME, DB_USER, DB_PASS) as conn:
        cursor = conn.cursor()
        
        # Specify the directory containing CSV files
        
        # Read CSV files based on the config
        csv_data = read_csv_files_based_on_config(data_directory, import_config["queries_config"])
        if csv_data:
            total_items = len(csv_data)
            for i, (report, content) in enumerate(csv_data.items(), start=1):
                logging.info(f"Processing data for {report} ({content['description']}), "
                             f"Month: {content['month']}, Year: {content['year']}")
                
                HMIS_CODE = content['data'][1][-1]  # Handle carefully, check row structure
                parts = report.split("_")
                report = "_".join(parts[:-3])  # Extract report name without month/year
                # Check for existing data to delete
                delete_existing = f"""
                    DELETE FROM {report}
                    WHERE HMISCode = '{HMIS_CODE}' AND ReportYear = '{content['year']}' AND ReportMonth = '{content['month']}'
                """
                try:
                    cursor.execute(delete_existing)
                    logging.info(
                        f"Deleted existing records for {report} with HMISCode {HMIS_CODE}, "
                        f"Year {content['year']}, Month {content['month']}")
                except Exception as e:
                    logging.error(f"Error executing delete query for {report}: {e}")
                    continue  # Skip to the next report if delete fails
                
                # Process data and insert dynamically
                try:
                    process_data_and_insert(cursor, content, report)
                except Exception as e:
                    logging.error(f"Error inserting data for {report}: {e}")
                    conn.rollback()
                    continue  # Skip to the next report if insert fails
                
                # Move processed file
                file_pattern = os.path.join(data_directory,
                                            f"*{content['data'][1][-2].replace(" ","")}{content['data'][1][-1]}_{content['month']}_{content['year']}.csv")
                move_imported_file(file_pattern)
                # Execute stored procedure
                curr_parameters = (
                    content['data'][1][-4], content['data'][1][-3], content['data'][1][-2],
                    content['data'][1][-1], content['month'], content['year'])
                sp_parameters.add(curr_parameters)
                print_progress_bar(i, total_items,
                                   prefix='Processing CSV files and inserting to DB',
                                   suffix=f"{i}/{total_items}",
                                   length=40)
                continue
        # Commit the transaction
        for i, sp_combination in enumerate(sp_parameters, start=1):
            total_sp = len(sp_parameters)
            print_progress_bar(i, total_sp, prefix='Executing SP',
                               suffix=f"{i}/{total_sp}",
                               length=40)
            region, zone, health_center, code, month_text, year = sp_combination
            try:
                month = month_mapping.get(month_text)
                year = int(year)
                gregorian_end_date = conv(year, month, 20)
                if month == 1:
                    gregorian_start_date = conv(year - 1, 12, 21)
                else:
                    gregorian_start_date = conv(year, month - 1, 21)
                sp_query = f"""
                                 EXEC SP_AggregateHivindicatorsAll '{region}', '{zone}', '{health_center}', '{code}', '{month_text}', '{year}','{gregorian_start_date}','{gregorian_end_date}'
                             """
                cursor.execute(sp_query)
                logging.info(f"Executed stored procedure for {sp_combination}")
            except Exception as e:
                logging.error(f"Error executing stored procedure for {report}: {e}")
                conn.rollback()
        conn.commit()
except Exception as e:
    logging.error(f"Critical error in main execution block: {e}")
finally:
    if cursor:
        cursor.close()
