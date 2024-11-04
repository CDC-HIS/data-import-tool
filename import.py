#!/usr/bin/env python3
import pytds
import csv
import glob
import os
import json

# Database connection parameters
DRIVER = "ODBC Driver 17 for SQL Server"
DB_HOST = "localhost"
DB_USER = "sa"
DB_PASS = "Abcd@1234"
DB_NAME = "AggregateDB"

with pytds.connect(DB_HOST, DB_NAME, DB_USER, DB_PASS) as conn:
    cursor = conn.cursor()
    cursor.execute("select * from Tx_Curr_AHD_LineList")
    for row in cursor.fetchall():
        print(row)


# Load configuration file for query name mappings
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        print(f"Configuration file {config_path} not found.")
        return {}
    except json.JSONDecodeError:
        print("Error decoding JSON configuration file.")
        return {}


# Read CSV files based on query_name mappings from config and include month/year
def read_csv_files_based_on_config(directory, config):
    file_pattern = os.path.join(directory, "*.csv")
    matching_files = glob.glob(file_pattern)
    
    if not matching_files:
        print("No CSV files found in the directory.")
        return None

    csv_data_by_query = {}

    for csv_file_path in matching_files:
        filename = os.path.basename(csv_file_path).replace(".csv", "")
        parts = filename.split("_")
        if len(parts) < 3:
            print(f"Skipping file with unexpected format: {filename}")
            continue
        
        query_name = "_".join(parts[:-2])  # Extract query name without month/year
        month = parts[-2]  # Extract the month part
        year = parts[-1]  # Extract the year part

        # Check if the query name is in the config file
        if query_name in config:
            # print(f"Processing file: {csv_file_path} (query_name: {query_name}, month: {month}, year: {year})")
            data = []
            with open(csv_file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                data.append(headers)
                for row in reader:
                    data.append(row)
            
            csv_data_by_query[query_name] = {
                "description": config[query_name],
                "month": month,
                "year": year,
                "data": data
            }
        else:
            print(f"Query name '{query_name}' not found in the configuration file.")

    return csv_data_by_query


# Load configuration
config_path = "output.json"
config = load_config(config_path)

# Specify the directory containing CSV files
directory = "output_csv"

# Read CSV files based on the config
csv_data = read_csv_files_based_on_config(directory, config)

# Print a preview of the loaded data
if csv_data:
    for query, content in csv_data.items():
        print(f"\nData for {query} ({content['description']}, Month: {content['month']}, Year: {content['year']}):")
        # for row in content['data'][:5]:  # Print the first 5 rows for preview
        if query == "TX_Curr_Line_List":
           insert_query = """
                            INSERT INTO Tx_Curr_LineList (
    Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age,
    FollowUpDate, FollowUpDate_GC, Next_visit_Date, Next_visit_Date_GC, ARVRegimen,
    RegimensLine, ARTDoseDays, FollowupStatus, ARTDoseEndDate, ARTDoseEndDate_DC,
    AdheranceLevel, ARTStartDate, ARTStartDate_GC, FP_Status, TB_SreeningStatus,
    ActiveTBDiagnosed, NutritionalScrenningStatus, SexForNutrition,
    TherapeuticFoodProvided, PatientGUID, IsPregnant, BreastFeeding, LMP_Date,
    LMP_Date_GC, MonthsOnART, ChildDisclosueStatus, DSD_Category
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?
);
"""
