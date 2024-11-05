#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import mysql.connector
import csv
import os
import sys
import json
from ethiopian_date import EthiopianDateConverter
from ttkthemes import ThemedTk

def resource_path(relative_path):
    """ Get the absolute path to the resource (works for development and PyInstaller) """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        return os.path.join(os.path.abspath("."), relative_path)

# Load query paths from a configuration file
query_config_path = resource_path("queries_config.json")
with open(query_config_path, 'r') as config_file:
    query_paths = json.load(config_file)

# MySQL credentials
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "Abcd@1234"
DB_NAME = "analysis_db"

root = ThemedTk()
root.get_themes()
root.set_theme("arc")
root.title("SQL Extraction Tool")
root.geometry("400x200")
root.eval('tk::PlaceWindow . center')

months = ["Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", "Yekatit", "Megabit", "Miyazia", "Ginbot", "Sene", "Hamle", "Nehase", "Puagume"]
month_mapping = {name: index + 1 for index, name in enumerate(months)}
facility_details_query = """
select state_province as Region, city_village as Woreda, mamba_dim_location.name as Facility from mamba_fact_location_tag
join mamba_fact_location_tag_map on mamba_fact_location_tag.location_tag_id=mamba_fact_location_tag_map.location_tag_id
join mamba_dim_location on mamba_dim_location.location_id = mamba_fact_location_tag_map.location_id where mamba_fact_location_tag.name='Facility Location';
"""
hmiscode_query="""
select value_reference as HMISCode from mamba_fact_location_attribute
join mamba_fact_location_attribute_type on mamba_fact_location_attribute.attribute_type_id=mamba_fact_location_attribute_type.location_attribute_type_id
where name='hmiscode';
"""
additional_columns = ['Region', 'Woreda', 'Facility', 'HMISCode']
def read_sql_file(file_path):
    """ Read and return the content of a SQL file """
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        messagebox.showerror("Error", f"SQL file {file_path} not found.")
        return None

def export_to_csv(queries, gregorian_start_date, gregorian_end_date):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cursor = conn.cursor()
        if not os.path.exists('output_csv'):
            os.makedirs('output_csv')
        for query_name, query in queries.items():
            cursor.execute(facility_details_query)
            facility_details = cursor.fetchall()
            cursor.execute(hmiscode_query)
            hmiscode = cursor.fetchall()
            formatted_query = query.replace("REPORT_END_DATE", f"'{gregorian_end_date}'").replace("REPORT_START_DATE", f"'{gregorian_start_date}'")
            cursor.execute(formatted_query)
            results = cursor.fetchall()
            modified_results = [row + (facility_details[0][0], facility_details[0][1],facility_details[0][2],hmiscode[0][0]) for row in results]
            csv_file_path = os.path.join('output_csv', f"{query_name}_{combo_month.get()}_{entry_year.get()}.csv")
            if modified_results:
                with open(csv_file_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([i[0] for i in cursor.description]+additional_columns)
                    writer.writerows(modified_results)
            else:
                messagebox.showwarning("Warning", f"No data returned for {query_name}.")

        messagebox.showinfo("Success", "Data exported to output_csv folder.")
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

def run_query():
    selected_month = combo_month.get()
    selected_year = entry_year.get()
    if not selected_month:
        messagebox.showerror("Error", "Please select a month.")
        return

    if not selected_year or len(selected_year) != 4 or not selected_year.isdigit():
        messagebox.showerror("Error", "Please enter a valid 4-digit year.")
        return

    month = month_mapping.get(selected_month)
    year = int(selected_year)
    conv = EthiopianDateConverter.to_gregorian

    gregorian_end_date = conv(year, month, 20)
    if month == 1:
        gregorian_start_date = conv(year - 1, 12, 21)
    else:
        gregorian_start_date = conv(year, month - 1, 21)

    queries = {}
    for tag, path in query_paths.items():
        query = read_sql_file(resource_path(path))
        if query:
            queries[tag] = query

    if queries:
        export_to_csv(queries, gregorian_start_date, gregorian_end_date)
    else:
        messagebox.showerror("Error", "No valid queries found.")

# UI Components
tk.Label(root, text="Select Month:").grid(row=3, column=0, pady=5, padx=10, sticky="e")
combo_month = ttk.Combobox(root, values=months, state="readonly", width=25)
combo_month.grid(row=3, column=1, padx=10, pady=5, sticky="w")

def validate_year(new_value):
    return new_value.isdigit() and len(new_value) <= 4

validate_cmd = (root.register(validate_year), '%P')
tk.Label(root, text="Year (YYYY):").grid(row=4, column=0, pady=5, padx=10, sticky="e")
entry_year = tk.Entry(root, validate="key", validatecommand=validate_cmd, width=25)
entry_year.grid(row=4, column=1, padx=10, pady=5, sticky="w")

run_button = tk.Button(root, text="Run Query", command=run_query)
run_button.grid(row=5, column=0, columnspan=2, pady=10)
run_button.config(width=20)

for i in range(3, 6):
    root.grid_columnconfigure(0, weight=1)
    root.grid_columnconfigure(1, weight=1)
    root.grid_rowconfigure(i, weight=1)

root.mainloop()
