#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk  # Import ttk for Combobox
import mysql.connector
import csv
import os  # Import os to handle file operations
from ethiopian_date import EthiopianDateConverter
from ttkthemes import ThemedTk
import os
import sys


def resource_path(relative_path):
    """ Get the absolute path to the resource (works for development and PyInstaller) """
    if hasattr(sys, '_MEIPASS'):
        # PyInstaller extracts files to a temp folder
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        # Running in development
        return os.path.join(os.path.abspath("."), relative_path)
    
sql_file_path = resource_path("queries.sql")

# MySQL credentials
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "Abcd@1234"
DB_NAME = "analysis_db"

root = ThemedTk()
root.get_themes()  # List available themes
root.set_theme("arc")  # Set the desired theme, e.g., "arc"

root.title("SQL Extraction Tool")
root.geometry("400x200")
root.eval('tk::PlaceWindow . center')  # Center the window on the screen

# List of months (converted to their corresponding numeric values)
months = [
    "Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", 
    "Yekatit", "Megabit", "Miyazia", "Ginbot", 
    "Sene", "Hamle", "Nehase", "Puagume"
]
query_tag = ["TX_Curr_Line_List","DataSheet_VL_Test_Received_Line_List",
             "AHD_Line_List","HVL_Line_List","VL_Eligibility_Line_List","TPT_Line_List"]

# Map month names to their respective numeric values
month_mapping = {name: index + 1 for index, name in enumerate(months)}

# Function to run the selected SQL query and export the result to separate CSV files
def export_to_csv(queries, gregorian_start_date, gregorian_end_date):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cursor = conn.cursor()
        print(gregorian_start_date, ' End ', gregorian_end_date)
        # Create a directory for CSV files if it doesn't exist
        if not os.path.exists('output_csv'):
            os.makedirs('output_csv')

        for query_name, query in queries.items():
            # Replace the placeholders :month and :year with actual values
            formatted_query = query.replace("END_DATE", f"'{gregorian_end_date}'").replace("START_DATE", f"'{gregorian_start_date}'")      
            cursor.execute(formatted_query)
            results = cursor.fetchall()

            # Construct the CSV file name
            csv_file_path = os.path.join('output_csv', f"{query_name}_{combo_month.get()}_{entry_year.get()}.csv")

            # Export to CSV
            if results:  # Only write to CSV if there are results
                with open(csv_file_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([i[0] for i in cursor.description])  # write headers
                    writer.writerows(results)
            else:
                messagebox.showwarning("Warning", f"No data returned for {query_name}.")

        messagebox.showinfo("Success", "Data exported to output_csv folder.")
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

# Function to build the SQL query based on user input and run it
def run_query():
    selected_month = combo_month.get()
    selected_year = entry_year.get()

    # Validation: Ensure that both the month and year are provided
    if not selected_month:
        messagebox.showerror("Error", "Please select a month.")
        return

    if not selected_year or len(selected_year) != 4 or not selected_year.isdigit():
        messagebox.showerror("Error", "Please enter a valid 4-digit year.")
        return

    # Get the month number from the selected month name
    month = month_mapping.get(selected_month)
    year = int(selected_year)
    conv = EthiopianDateConverter.to_gregorian

    gregorian_end_date = conv(year, month, 20)
    
    # Handle the month - 1 scenario
    if month == 1:  # If it's Meskerem, decrement the month and adjust the year
        gregorian_start_date = conv(year - 1, 12, 21)  # Puagume is the 13th month
    else:
        gregorian_start_date = conv(year, month - 1, 21)
    # Read queries from external file
    queries = {}
    try:
        with open(sql_file_path, "r") as f:
            query_data = f.read().strip().split(";")
            for i, q in enumerate(query_data):
                q = q.strip()
                if q:  # Only add non-empty queries
                    queries[query_tag[i]] = q
        
        if queries:  # Check if any queries exist
            export_to_csv(queries, gregorian_start_date, gregorian_end_date)
        else:
            messagebox.showerror("Error", "No valid queries found.")
    except FileNotFoundError:
        messagebox.showerror("Error", "Query file not found.")

# Create the main application window

# Month selection using Combobox
tk.Label(root, text="Select Month:").grid(row=3, column=0, pady=5, padx=10, sticky="e")
combo_month = ttk.Combobox(root, values=months, state="readonly", width=25)  # Increase width
combo_month.grid(row=3, column=1, padx=10, pady=5, sticky="w")

# Validation function to allow only four digits in the year entry
def validate_year(new_value):
    return new_value.isdigit() and len(new_value) <= 4

validate_cmd = (root.register(validate_year), '%P')  # Register the validation command

# Year input
tk.Label(root, text="Year (YYYY):").grid(row=4, column=0, pady=5, padx=10, sticky="e")
entry_year = tk.Entry(root, validate="key", validatecommand=validate_cmd, width=25)  # Increase width
entry_year.grid(row=4, column=1, padx=10, pady=5, sticky="w")

# Button to run the query
run_button = tk.Button(root, text="Run Query", command=run_query)
run_button.grid(row=5, column=0, columnspan=2, pady=10)  # Center the button across columns
run_button.config(width=20)  # Set a fixed width for the button

# Center all elements within the grid
for i in range(3, 6):  # Rows where labels and inputs are
    root.grid_columnconfigure(0, weight=1)  # Make column 0 expandable
    root.grid_columnconfigure(1, weight=1)  # Make column 1 expandable
    root.grid_rowconfigure(i, weight=1)  # Make rows 3-5 expandable

# Start the Tkinter main loop
root.mainloop()
