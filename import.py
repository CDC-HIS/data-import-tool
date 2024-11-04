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


# Load configuration file for report name mappings
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


# Read CSV files based on report_name mappings from config and include month/year
def read_csv_files_based_on_config(directory, config):
    file_pattern = os.path.join(directory, "*.csv")
    matching_files = glob.glob(file_pattern)

    if not matching_files:
        print("No CSV files found in the directory.")
        return None

    csv_data_by_report = {}

    for csv_file_path in matching_files:
        filename = os.path.basename(csv_file_path).replace(".csv", "")
        parts = filename.split("_")
        if len(parts) < 3:
            print(f"Skipping file with unexpected format: {filename}")
            continue

        report_name = "_".join(parts[:-2])  # Extract report name without month/year
        month = parts[-2]  # Extract the month part
        year = parts[-1]  # Extract the year part

        # Check if the report name is in the config file
        if report_name in config:
            # Read the CSV file
            data = []
            with open(csv_file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                data.append(headers)
                for row in reader:
                    data.append(row)

            csv_data_by_report[report_name] = {
                "description": config[report_name],
                "month": month,
                "year": year,
                "data": data
            }
        else:
            print(f"Report name '{report_name}' not found in the configuration file.")

    return csv_data_by_report


# Main execution block
with pytds.connect(DB_HOST, DB_NAME, DB_USER, DB_PASS) as conn:
    cursor = conn.cursor()

    # Load configuration
    config_path = "output.json"
    config = load_config(config_path)

    # Specify the directory containing CSV files
    directory = "output_csv"

    # Read CSV files based on the config
    csv_data = read_csv_files_based_on_config(directory, config)

    # Print a preview of the loaded data
    if csv_data:
        for report, content in csv_data.items():
            # print(f"\nData for {report} ({content['description']}, Month: {content['month']}, Year: {content['year']}):")
            if report == "TX_Curr_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month'])
                cursor.execute(delete_existing)
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
                    %(Region)s, %(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s,
                    %(Sex)s, %(Weight)s, %(Age)s, %(FollowUpDate)s, %(FollowUpDate_GC)s, %(Next_visit_Date)s,
                    %(Next_visit_Date_GC)s, %(ARVRegimen)s, %(RegimensLine)s, %(ARTDoseDays)s,
                    %(FollowupStatus)s, %(ARTDoseEndDate)s, %(ARTDoseEndDate_DC)s, %(AdheranceLevel)s,
                    %(ARTStartDate)s, %(ARTStartDate_GC)s, %(FP_Status)s, %(TB_SreeningStatus)s,
                    %(ActiveTBDiagnosed)s, %(NutritionalScrenningStatus)s, %(SexForNutrition)s,
                    %(TherapeuticFoodProvided)s, %(PatientGUID)s, %(IsPregnant)s, %(BreastFeeding)s,
                    %(LMP_Date)s, %(LMP_Date_GC)s, %(MonthsOnART)s, %(ChildDisclosueStatus)s, %(DSD_Category)s
                );
                """

                # Execute insert for each row in the data
                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'region',
                        "Woreda": 'woreda',
                        "Facility": 'facility',
                        "HMISCode": 'hmis',
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[0],
                        "Weight": row[1],
                        "Age": row[2],
                        "FollowUpDate": row[3],
                        "FollowUpDate_GC": row[4],
                        "Next_visit_Date": row[5],
                        "Next_visit_Date_GC": row[6],
                        "ARVRegimen": row[7],
                        "RegimensLine": row[7][:2],
                        "ARTDoseDays": row[8],
                        "FollowupStatus": row[9],
                        "ARTDoseEndDate": row[10],
                        "ARTDoseEndDate_DC": row[11],
                        "AdheranceLevel": row[12],
                        "ARTStartDate": row[13],
                        "ARTStartDate_GC": row[14],
                        "FP_Status": row[15],
                        "TB_SreeningStatus": row[16],
                        "ActiveTBDiagnosed": row[17],
                        "NutritionalScrenningStatus": row[18],
                        "SexForNutrition": row[19],
                        "TherapeuticFoodProvided": row[20],
                        "PatientGUID": row[21],
                        "IsPregnant": row[22],
                        "BreastFeeding": row[23],
                        "LMP_Date": row[24],
                        "LMP_Date_GC": row[25],
                        "MonthsOnART": row[26],
                        "ChildDisclosueStatus": row[27],
                        "DSD_Category": row[29],
                    }
                    cursor.execute(insert_query, values)
            if report == "TPT_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_TPT_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month']
                )

                # Execute the DELETE statement
                try:
                    cursor.execute(delete_existing)
                except Exception as e:
                    print(f"Error executing delete statement: {e}")

                # Define the INSERT query
                insert_query = """
                INSERT INTO Tx_Curr_TPT_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, TPT_Started_Date, TPT_Completed_Date, TPT_Type, TPT_TypeAlt, TPT_TypeChar, HIV_Confirmed_Date, ART_Start_Date, FollowUpDate, Transfer_In, ARTDoseDays, Next_visit_Date, FollowupStatus, FollowupStatusChar, ARTDoseEndDate, PatientGUID, WHOStage, AdultCD4Count, ChildCD4Count, CPT_StartDate, CPT_StartDate_GC, CPT_StopDate, CPT_StopDate_GC, TB_SpecimenType, ActiveTBDiagnosed, ActiveTBDignosedDate, ActiveTBDignosedDate_GC, TBTx_StartDate, TBTx_StartDate_GC, TBTx_CompletedDate, TBTx_CompletedDate_GC, FluconazoleStartDate, FluconazoleStartDate_GC, FluconazoleEndDate, FluconazoleEndDate_GC)
                VALUES (%(Region)s, %(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s, %(Sex)s, %(Weight)s, %(Age)s, %(TPT_Started_Date)s, %(TPT_Completed_Date)s, %(TPT_Type)s, %(TPT_TypeAlt)s, %(TPT_TypeChar)s, %(HIV_Confirmed_Date)s, %(ART_Start_Date)s, %(FollowUpDate)s, %(Transfer_In)s, %(ARTDoseDays)s, %(Next_visit_Date)s, %(FollowupStatus)s, %(FollowupStatusChar)s, %(ARTDoseEndDate)s, %(PatientGUID)s, %(WHOStage)s, %(AdultCD4Count)s, %(ChildCD4Count)s, %(CPT_StartDate)s, %(CPT_StartDate_GC)s, %(CPT_StopDate)s, %(CPT_StopDate_GC)s, %(TB_SpecimenType)s, %(ActiveTBDiagnosed)s, %(ActiveTBDignosedDate)s, %(ActiveTBDignosedDate_GC)s, %(TBTx_StartDate)s, %(TBTx_StartDate_GC)s, %(TBTx_CompletedDate)s, %(TBTx_CompletedDate_GC)s, %(FluconazoleStartDate)s, %(FluconazoleStartDate_GC)s, %(FluconazoleEndDate)s, %(FluconazoleEndDate_GC)s);
                """
                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'region',
                        "Woreda": 'woreda',
                        "Facility": 'facility',
                        "HMISCode": 'hmis',
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[0],
                        "Weight": row[1],
                        "Age": row[2],
                        "TPT_Started_Date": row[3],
                        "TPT_Completed_Date": row[4],
                        "TPT_Type": row[5],
                        "TPT_TypeAlt": row[6],
                        "TPT_TypeChar": row[7],
                        "HIV_Confirmed_Date": row[8],
                        "ART_Start_Date": row[9],
                        "FollowUpDate": row[10],
                        "Transfer_In": 'ti',
                        "ARTDoseDays": row[11],
                        "Next_visit_Date": row[12],
                        "FollowupStatus": row[13][:2],
                        "FollowupStatusChar": row[14],
                        "ARTDoseEndDate": row[15],
                        "PatientGUID": row[16],
                        "WHOStage": row[17][:10],
                        "AdultCD4Count": row[18],
                        "ChildCD4Count": row[19],
                        "CPT_StartDate": row[20],
                        "CPT_StartDate_GC": row[21],
                        "CPT_StopDate": row[22],
                        "CPT_StopDate_GC": row[23],
                        "TB_SpecimenType": row[24],
                        "ActiveTBDiagnosed": row[25],
                        "ActiveTBDignosedDate": row[26],
                        "ActiveTBDignosedDate_GC": row[27],
                        "TBTx_StartDate": row[28],
                        "TBTx_StartDate_GC": row[29],
                        "TBTx_CompletedDate": row[30],
                        "TBTx_CompletedDate_GC": row[31],
                        "FluconazoleStartDate": row[32],
                        "FluconazoleStartDate_GC": row[33],
                        "FluconazoleEndDate": row[34],
                        "FluconazoleEndDate_GC": row[35],
                    }
                    cursor.execute(insert_query, values)
            if report == "AHD_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_AHD_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month'])
                # Execute the DELETE statement
                try:
                    cursor.execute(delete_existing)
                except Exception as e:
                    print(f"Error executing delete statement: {e}")
                insert_query = """
                INSERT INTO Tx_Curr_AHD_LineList (Region,Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, PatientGUID, Height, HIV_Confirmed_Date, ARTStartDate, MonthsOnART, FollowUpDate, WHOStage, CD4Count, ARTDoseDays, ARVRegimen, FollowupStatus, AdheranceLevel, IsPregnant, FpMethodUsed, CrAg, NutritionalStatus, FunctionalStatus, No_OI, Zoster, Bacterial_Pneumonia, Extra_Pulmonary_TB, Oesophageal_Candidiasis, Vaginal_Candidiasis, Mouth_Ulcer, Chronic_Diarrhea, Acute_Diarrhea, CNS_Toxoplasmosis, Cryptococcal_Meningitis, Kaposi_Sarcoma, Cervical_Cancer, Pulmonary_TB, Oral_Candidiasis, Pneumocystis_Pneumonia, NonHodgkins_Lymphoma, Genital_Ulcer, OI_Other, Med1, Med2, CotrimoxazoleStartDate, cortimoxazole_stop_date, Fluconazole_Start_Date, Fluconazole_End_Date, TPT_Type, inhprophylaxis_started_date, InhprophylaxisCompletedDate,
                        TPT_DoseDaysNumberALT, TPT_DoseDaysNumberINH, TPT_Dispensed_Dose, TPT_SideEffect, TPT_Adherence, tb_screened, tb_screening_result, TB_Diagnostic_Result, LF_LAM_result, Gene_Xpert_result, Smear_Microscopy_Result, Additional_TB_Diagnostic_Test_Result, Active_TB, ActiveTBTreatmentStartDate, ActiveTBTreatmentCompletedDate, ActiveTBTreatmentDiscontinuedDate, Viral_Load_Perform_Date, Viral_Load_Status, Viral_Load_count, VL_Sent_Date, Viral_Load_Ref_Date, CCA_Screened, DSD_Category, AHD)
                        VALUES (%(Region)s,%(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s, %(Sex)s, %(Weight)s, %(Age)s, %(PatientGUID)s, %(Height)s, %(HIV_Confirmed_Date)s, %(ARTStartDate)s, %(MonthsOnART)s, %(FollowUpDate)s, %(WHOStage)s, %(CD4Count)s, %(ARTDoseDays)s, %(ARVRegimen)s, %(FollowupStatus)s, %(AdheranceLevel)s, %(IsPregnant)s, %(FpMethodUsed)s, %(CrAg)s, %(NutritionalStatus)s, %(FunctionalStatus)s, %(No_OI)s, %(Zoster)s, %(Bacterial_Pneumonia)s, %(Extra_Pulmonary_TB)s, %(Oesophageal_Candidiasis)s, %(Vaginal_Candidiasis)s, %(Mouth_Ulcer)s, %(Chronic_Diarrhea)s, %(Acute_Diarrhea)s, %(CNS_Toxoplasmosis)s, %(Cryptococcal_Meningitis)s, %(Kaposi_Sarcoma)s, %(Cervical_Cancer)s, %(Pulmonary_TB)s, %(Oral_Candidiasis)s, %(Pneumocystis_Pneumonia)s, %(NonHodgkins_Lymphoma)s, %(Genital_Ulcer)s, %(OI_Other)s, %(Med1)s, %(Med2)s, %(CotrimoxazoleStartDate)s, %(cortimoxazole_stop_date)s, %(Fluconazole_Start_Date)s, %(Fluconazole_End_Date)s, %(TPT_Type)s, %(inhprophylaxis_started_date)s, %(InhprophylaxisCompletedDate)s,
                       %(TPT_DoseDaysNumberALT)s, %(TPT_DoseDaysNumberINH)s, %(TPT_Dispensed_Dose)s, %(TPT_SideEffect)s, %(TPT_Adherence)s, %(tb_screened)s, %(tb_screening_result)s, %(TB_Diagnostic_Result)s, %(LF_LAM_result)s, %(Gene_Xpert_result)s, %(Smear_Microscopy_Result)s, %(Additional_TB_Diagnostic_Test_Result)s, %(Active_TB)s, %(ActiveTBTreatmentStartDate)s, %(ActiveTBTreatmentCompletedDate)s,%(ActiveTBTreatmentDiscontinuedDate)s, %(Viral_Load_Perform_Date)s, %(Viral_Load_Status)s, %(Viral_Load_count)s, %(VL_Sent_Date)s, %(Viral_Load_Ref_Date)s, %(CCA_Screened)s, %(DSD_Category)s, %(AHD)s);
                """

                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'Region',
                        "Woreda": 'woreda',
                        "Facility": 'facility',
                        "HMISCode": HMIS_CODE,
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[0],
                        "Weight": row[1],
                        "Age": row[2],
                        "PatientGUID": row[3],
                        "Height": row[4],
                        "HIV_Confirmed_Date": row[5],
                        "ARTStartDate": row[6],
                        "MonthsOnART": row[7],
                        "FollowUpDate": row[8],
                        "WHOStage": row[9][:20],
                        "CD4Count": row[10],
                        "ARTDoseDays": row[11],
                        "ARVRegimen": row[12],
                        "FollowupStatus": row[13],
                        "AdheranceLevel": row[14],
                        "IsPregnant": row[15],
                        "FpMethodUsed": row[16],
                        "CrAg": row[17],
                        "NutritionalStatus": row[18][:50],
                        "FunctionalStatus": row[19],
                        "No_OI": row[20],
                        "Zoster": row[21],
                        "Bacterial_Pneumonia": row[22],
                        "Extra_Pulmonary_TB": row[23],
                        "Oesophageal_Candidiasis": row[24],
                        "Vaginal_Candidiasis": row[25],
                        "Mouth_Ulcer": row[26],
                        "Chronic_Diarrhea": row[27],
                        "Acute_Diarrhea": row[28],
                        "CNS_Toxoplasmosis": row[29],
                        "Cryptococcal_Meningitis": row[30],
                        "Kaposi_Sarcoma": row[31],
                        "Cervical_Cancer": row[32],
                        "Pulmonary_TB": row[33],
                        "Oral_Candidiasis": row[34],
                        "Pneumocystis_Pneumonia": row[35],
                        "NonHodgkins_Lymphoma": row[36],
                        "Genital_Ulcer": row[37],
                        "OI_Other": row[38],
                        "Med1": row[39],
                        "Med2": row[40],
                        "CotrimoxazoleStartDate": row[41],
                        "cortimoxazole_stop_date": row[42],
                        "Fluconazole_Start_Date": row[43],
                        "Fluconazole_End_Date": row[44],
                        "TPT_Type": row[45],
                        "inhprophylaxis_started_date": row[46],
                        "InhprophylaxisCompletedDate": row[47],
                        "TPT_DoseDaysNumberALT": row[48],
                        "TPT_DoseDaysNumberINH": row[49],
                        "TPT_Dispensed_Dose": row[50],
                        "TPT_SideEffect": row[51],
                        "TPT_Adherence": row[52],
                        "tb_screened": row[53],
                        "tb_screening_result": row[54],
                        "TB_Diagnostic_Result": row[55],
                        "LF_LAM_result": row[56],
                        "Gene_Xpert_result": row[57],
                        "Smear_Microscopy_Result": row[58],
                        "Additional_TB_Diagnostic_Test_Result": row[59],
                        "Active_TB": row[60],
                        "ActiveTBTreatmentStartDate": row[61],
                        "ActiveTBTreatmentCompletedDate": row[62],
                        "ActiveTBTreatmentDiscontinuedDate": row[63],
                        "Viral_Load_Perform_Date": row[64],
                        "Viral_Load_Status": row[65][:20],
                        "Viral_Load_count": row[66],
                        "VL_Sent_Date": row[67],
                        "Viral_Load_Ref_Date": row[68],
                        "CCA_Screened": row[69][:10],
                        "DSD_Category": row[70],
                        "AHD": row[71]
                    }
                    cursor.execute(insert_query, values)
            if report == "DataSheet_VL_Test_Received_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_VLTestReceived_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month'])
                # Execute the DELETE statement
                try:
                    cursor.execute(delete_existing)
                except Exception as e:
                    print(f"Error executing delete statement: {e}")
                insert_query = """
                INSERT INTO Tx_Curr_VLTestReceived_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, Breastfeeding, ARVDispendsedDose, ARVRegimensLine, ARTDoseDays, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_ref_date, ReasonForVLTest, PMTCT_ART, PatientGUID )
                VALUES (%(Region)s, %(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s, %(Sex)s, %(Weight)s, %(Age)s, %(date_hiv_confirmed)s, %(art_start_date)s, %(FollowUpDate)s, %(IsPregnant)s, %(Breastfeeding)s, %(ARVDispendsedDose)s, %(ARVRegimensLine)s, %(ARTDoseDays)s, %(next_visit_date)s, %(follow_up_status)s, %(art_dose_End)s, %(viral_load_perform_date)s, %(viral_load_status)s, %(viral_load_count)s, %(viral_load_ref_date)s, %(ReasonForVLTest)s, %(PMTCT_ART)s, %(PatientGUID)s )
                """
                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'Region',
                        "Woreda": 'Woreda',
                        "Facility": 'Facility',
                        "HMISCode": 'HMISCode',
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[0],
                        "Weight": row[1],
                        "Age": row[2],
                        "date_hiv_confirmed": row[3],
                        "art_start_date": row[4],
                        "FollowUpDate": row[5],
                        "IsPregnant": row[6],
                        "Breastfeeding": row[7],
                        "ARVDispendsedDose": row[8],
                        "ARVRegimensLine": row[9][:2],
                        "ARTDoseDays": row[10],
                        "next_visit_date": row[11],
                        "follow_up_status": row[12],
                        "art_dose_End": row[13],
                        "viral_load_perform_date": row[14],
                        "viral_load_status": row[15],
                        "viral_load_count": row[16],
                        "viral_load_ref_date": row[17],
                        "ReasonForVLTest": row[18],
                        "PMTCT_ART": row[19],
                        "PatientGUID": row[20]
                    }
                    cursor.execute(insert_query, values)
            if report == "HVL_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_HVL_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month'])
                # Execute the DELETE statement
                try:
                    cursor.execute(delete_existing)
                except Exception as e:
                    print(f"Error executing delete statement: {e}")
                insert_query = """
                INSERT INTO Tx_Curr_HVL_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age,
                 date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, ARVDispendsedDose, art_dose, next_visit_date, 
                 follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_sent_date, 
                 viral_load_ref_date, routine_viral_load, target, date_regimen_change, date_eac_provided_1, date_eac_provided_2,
                  date_eac_provided_3,viral_load_sent_date_cf, viral_load_perform_date_cf, viral_load_status_cf, viral_load_count_cf, 
                  routine_viral_load_cf, target_cf, PatientGUID)
               VALUES (
                     %(Region)s, %(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s, %(Sex)s, %(Weight)s, %(Age)s,
                    %(date_hiv_confirmed)s, %(art_start_date)s, %(FollowUpDate)s, %(IsPregnant)s, %(ARVDispendsedDose)s, %(art_dose)s, %(next_visit_date)s,
                    %(follow_up_status)s, %(art_dose_End)s, %(viral_load_perform_date)s, %(viral_load_status)s, %(viral_load_count)s, %(viral_load_sent_date)s,
                    %(viral_load_ref_date)s, %(routine_viral_load)s, %(target)s, %(date_regimen_change)s, %(date_eac_provided_1)s, %(date_eac_provided_2)s,
                    %(date_eac_provided_3)s, %(viral_load_sent_date_cf)s, %(viral_load_perform_date_cf)s, %(viral_load_status_cf)s, %(viral_load_count_cf)s,
                    %(routine_viral_load_cf)s, %(target_cf)s, %(PatientGUID)s        );
                """

                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'Region',
                        "Woreda": 'Woreda',
                        "Facility": 'Facility',
                        "HMISCode": 'HMISCode',
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[0],
                        "Weight": row[1],
                        "Age": row[2],
                        "date_hiv_confirmed": row[3],
                        "art_start_date": row[4],
                        "FollowUpDate": row[5],
                        "IsPregnant": row[6],
                        "ARVDispendsedDose": row[8][:10],
                        "art_dose": row[7],
                        "next_visit_date": row[9],
                        "follow_up_status": row[10],
                        "art_dose_End": row[11],
                        "viral_load_perform_date": row[12],
                        "viral_load_status": row[13],
                        "viral_load_count": row[14],
                        "viral_load_sent_date": row[15],
                        "viral_load_ref_date": row[16],
                        "routine_viral_load": row[17][:100],
                        "target": row[18],
                        "date_regimen_change": row[19],
                        "date_eac_provided_1": row[20],
                        "date_eac_provided_2": row[21],
                        "date_eac_provided_3": row[22],
                        "viral_load_sent_date_cf": row[23],
                        "viral_load_perform_date_cf": row[24],
                        "viral_load_status_cf": row[25],
                        "viral_load_count_cf": row[26],
                        "routine_viral_load_cf": row[27][:100],
                        "target_cf": row[28],
                        "PatientGUID": row[29]
                    }
                    cursor.execute(insert_query, values)
            if report == "VL_Eligibility_Line_List":
                HMIS_CODE = 'hmis'
                delete_existing = "DELETE FROM Tx_Curr_VLEligible_LineList WHERE HMISCode = '{}' AND ReportYear = '{}' AND ReportMonth = '{}'".format(
                    HMIS_CODE, content['year'], content['month'])
                # Execute the DELETE statement
                try:
                    cursor.execute(delete_existing)
                except Exception as e:
                    print(f"Error executing delete statement: {e}")
                insert_query = """
                    INSERT INTO Tx_Curr_VLEligible_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, ARVDispendsedDose, art_dose, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_sent_date, viral_load_ref_date, date_regimen_change, eligiblityDate, PatientGUID )
                    VALUES (%(Region)s, %(Woreda)s, %(Facility)s, %(HMISCode)s, %(ReportYear)s, %(ReportMonth)s, %(Sex)s, %(Weight)s, %(Age)s, 
                            %(date_hiv_confirmed)s, %(art_start_date)s, %(FollowUpDate)s, %(IsPregnant)s, %(ARVDispendsedDose)s, %(art_dose)s, 
                            %(next_visit_date)s, %(follow_up_status)s, %(art_dose_End)s, %(viral_load_perform_date)s, %(viral_load_status)s, 
                            %(viral_load_count)s, %(viral_load_sent_date)s, %(viral_load_ref_date)s, %(date_regimen_change)s, %(eligiblityDate)s, %(PatientGUID)s
                            )
                """
                for row in content['data'][1:]:  # Skip headers
                    values = {
                        "Region": 'Region',
                        "Woreda": 'Woreda',
                        "Facility": 'Facility',
                        "HMISCode": 'HMISCode',
                        "ReportYear": content['year'],
                        "ReportMonth": content['month'],
                        "Sex": row[13],
                        "Weight": row[19],
                        "Age": row[18],
                        "date_hiv_confirmed": row[3],
                        "art_start_date": row[1],
                        "FollowUpDate": row[6],
                        "IsPregnant": row[8],
                        "ARVDispendsedDose": row[21][:10],
                        "art_dose": row[20],
                        "next_visit_date": row[22],
                        "follow_up_status": row[5],
                        "art_dose_End": row[23],
                        "viral_load_perform_date": row[15],
                        "viral_load_status": row[17],
                        "viral_load_count": row[14],
                        "viral_load_sent_date": row[16],
                        "viral_load_ref_date": row[24],
                        "date_regimen_change": row[25],
                        "eligiblityDate": row[28],
                        "PatientGUID": row[11]
                    }
                    cursor.execute(insert_query, values)

    # Commit the transaction
    conn.commit()
    cursor.close()
