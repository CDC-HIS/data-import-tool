           'Delete data from Tx_Curr_LineLis
                mssql = "delete from Tx_Curr_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"
                
                 'Delete data from Tx_Curr_OutComeList
                mssql = "delete from Tx_Curr_OutComeList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"
                                     
                'Delete data from Tx_Curr_TPT_LineList
                mssql = "delete from Tx_Curr_TPT_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_CCA_LineList
                mssql = "delete from Tx_Curr_CCA_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_CCANew_LineList
                mssql = "delete from Tx_Curr_CCANew_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"
 
                'Delete data from Tx_Curr_VLEligible_LineList
                mssql = "delete from Tx_Curr_VLEligible_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_VLEligibleNew_LineList
                mssql = "delete from Tx_Curr_VLEligibleNew_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_VLTestReceived_LineList
                mssql = "delete from Tx_Curr_VLTestReceived_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_HVL_LineList
                mssql = "delete from Tx_Curr_HVL_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"

                'Delete data from Tx_Curr_AHD_LineList
                mssql = "delete from Tx_Curr_AHD_LineList WHERE HMISCode = '' AND ReportYear = '' AND ReportMonth = ''"


'Insert TX_Curr_LineList'
                mssql = "INSERT INTO Tx_Curr_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, FollowUpDate, FollowUpDate_GC, Next_visit_Date, Next_visit_Date_GC, ARVRegimen, RegimensLine, ARTDoseDays, FollowupStatus, ARTDoseEndDate, ARTDoseEndDate_DC, AdheranceLevel, ARTStartDate, ARTStartDate_GC, FP_Status, TB_SreeningStatus, ActiveTBDiagnosed, NutritionalScrenningStatus, SexForNutrition, TherapeuticFoodProvided, PatientGUID, IsPregnant, BreastFeeding, LMP_Date, LMP_Date_GC, MonthsOnART, ChildDisclosueStatus, DSD_Category) " & _
                "VALUES ('');"
                
            
                mssql = "INSERT INTO Tx_Curr_OutComeList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, TOs ,Losts,Drops,Deads,Stops,Not_Updated,Traced_Back,Restarts,TI,New,TOsPedi,LostsPedi,DropsPedi,DeadsPedi,StopsPedi,Not_UpdatedPedi,Traced_BackPedi,RestartsPedi,TIPedi,NewPedi,AgeOut) " & _
                "VALUES ('');"
            
    End With
    
        With ThisWorkbook.Sheets("DataSheetTPT")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_TPT_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, TPT_Started_Date, TPT_Completed_Date, TPT_Type, TPT_TypeAlt, TPT_TypeChar, HIV_Confirmed_Date, ART_Start_Date, FollowUpDate, Transfer_In, ARTDoseDays, Next_visit_Date, FollowupStatus, FollowupStatusChar, ARTDoseEndDate, PatientGUID, WHOStage, AdultCD4Count, ChildCD4Count, CPT_StartDate, CPT_StartDate_GC, CPT_StopDate, CPT_StopDate_GC, TB_SpecimenType, ActiveTBDiagnosed, ActiveTBDignosedDate, ActiveTBDignosedDate_GC, TBTx_StartDate, TBTx_StartDate_GC, TBTx_CompletedDate, TBTx_CompletedDate_GC, FluconazoleStartDate, FluconazoleStartDate_GC, FluconazoleEndDate, FluconazoleEndDate_GC) " & _
                "VALUES ('');"
                
              
            
            Next row
        
        End With
   
    
        With ThisWorkbook.Sheets("DataSheetCCA")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_CCA_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, CCS_OfferedYes, CCS_OfferedNo, CCS_AcceptedYes, CCS_AcceptedNo, CCS_ScreenDoneYes, CCS_ScreenDoneNo, CCS_ScreenDone_Date, CCS_Screen_Type, CCS_Screen_Method, CCS_HPV_Result, CCS_VIA_Result, CCS_Precancerous_Treat, CCS_Suspicious_Treat, CCS_Treat_Received_Date, CCS_Next_Date, date_hiv_confirmed, art_start_date, FollowUpDate, Transfer_In, ARTDoseDays, next_visit_date, follow_up_status, FollowupStatusChar, ARTDoseEndDate, PatientGUID) " & _
                "VALUES ('');"
             
            Next row
        
        End With
       
        With ThisWorkbook.Sheets("DataSheetCCANew")
                    For row = 4 To .Range("E" & 2).Value + 3

                'mssql = "INSERT INTO Tx_Curr_CCANew_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, FollowUpDate, ArtStartDate, FollowUpStatus, next_visit_date, ARVRegimen, RegimenLine, ARTDoseDays, Prev_CSS_Screen_Done_Date_Calculated, Prev_AppointmentDate_4_CCS, EligibilityReason, Prev_Screen_Type, Prev_Screen_Method, Prev_HPV_SubType, Prev_CCS_Screen_Result, Prev_CxCA_TreatmentGiven, Prev_CxCaTreatmentGivenDate, Seen, Curr_CSS_Screen_Done_Date_Calculated, Counselled, Accepted, Curr_Screen_Type, Curr_Screen_Method, Curr_HPV_SubType, Curr_CCS_Screen_Result, Curr_CxCa_TreatmentGiven, CxCaTreatmentGivenDate, PatientGUID)
                mssql = "INSERT INTO Tx_Curr_CCANew_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, FollowUpDate, ArtStartDate, FollowUpStatus, next_visit_date, ARVRegimen, RegimenLine, ARTDoseDays, Prev_CSS_Screen_Done_Date_Calculated, Prev_AppointmentDate_4_CCS, EligibilityReason, Prev_Screen_Type, Prev_Screen_Method, Prev_HPV_SubType, Prev_HPV_DAN_SampleCollected_Date, Prev_HPV_DAN_ResultReceived_Date, Prev_HPV_Result, Prev_VIA_Screening_Date, Prev_VIA_Screening_Result, Prev_Cytology_SampleCollected_Date, Prev_Cytology_ResultReceived_Date, Prev_Cytology_Result, Prev_Colposcopy_Exam_Date, Prev_Colposcopy_Exam_Result, Prev_Biopsy_SampleCollected_Date, Prev_Biopsy_ResultReceived_Date, Prev_Biopsy_Result, Prev_TX_Received_for_PrecancerousLesion, Prev_TX_for_ConfirmedCxCaBasedOn_Biopsy, Prev_Date_TX_Given, Prev_ReferralStatus, Prev_Reason_for_Referral, Prev_Date_Referred_to_OtherHF, Prev_Date_Client_Arrived_in_RefferedHF, " & _
                        "Prev_Date_Client_Served_in_RefferedHF, Prev_CCS_Screen_Result, Seen, Curr_CSS_Screen_Done_Date_Calculated, Counselled, Accepted, Curr_Screen_Type, Curr_Screen_Method, Curr_HPV_SubType, Curr_HPV_DAN_SampleCollected_Date, Curr_HPV_DAN_ResultReceived_Date, Curr_HPV_Result, Curr_VIA_Screening_Date, Curr_VIA_Screening_Result, Curr_Cytology_SampleCollected_Date, Curr_Cytology_ResultReceived_Date, Curr_Cytology_Result, Curr_Colposcopy_Exam_Date, Curr_Colposcopy_Exam_Result, Curr_Biopsy_SampleCollected_Date, Curr_Biopsy_ResultReceived_Date, Curr_Biopsy_Result, Curr_TX_Received_for_PrecancerousLesion, Curr_TX_for_ConfirmedCxCaBasedOn_Biopsy, Curr_Date_TX_Given, Curr_ReferralStatus, Curr_Reason_for_Referral, Curr_Date_Referred_to_OtherHF, Curr_Date_Client_Arrived_in_RefferedHF, Curr_Date_Client_Served_in_RefferedHF, Curr_CCS_Screen_Result, Next_AppointmentDate_4_CCS, PatientGUID) " & _
                "VALUES ('');"
                
             
            Next row
        
        End With
        
       
        With ThisWorkbook.Sheets("DataSheetVLEligible")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_VLEligible_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, ARVDispendsedDose, art_dose, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_sent_date, viral_load_ref_date, date_regimen_change, eligiblityDate, PatientGUID ) " & _
                "VALUES ('');"
                
          
            Next row
        
        End With
        
       
        With ThisWorkbook.Sheets("DataSheetVLEligibleNew")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_VLEligibleNew_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, ARVDispendsedDose, ARTDoseDays, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_sent_date, viral_load_ref_date, date_regimen_change, eligiblityDate, PatientGUID,IsBreastfeeding, PMTCT_ART ) " & _
                "VALUES ('');"
                
              
           
                Next row
             End With
            
            With ThisWorkbook.Sheets("DataSheetVLTestReceived")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_VLTestReceived_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, Breastfeeding, ARVDispendsedDose, ARVRegimensLine, ARTDoseDays, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_ref_date, ReasonForVLTest, PMTCT_ART, PatientGUID ) " & _
                "VALUES ('');"
                
           
                Next row
        
            End With
        
        
            With ThisWorkbook.Sheets("DataSheetHVL")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_HVL_LineList (Region, Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, date_hiv_confirmed, art_start_date, FollowUpDate, IsPregnant, ARVDispendsedDose, art_dose, next_visit_date, follow_up_status, art_dose_End, viral_load_perform_date, viral_load_status, viral_load_count, viral_load_sent_date, viral_load_ref_date, routine_viral_load, target, date_regimen_change, date_eac_provided_1, date_eac_provided_2, date_eac_provided_3, date_eac_provided_4, date_eac_provided_5, date_eac_provided_6, viral_load_sent_date_cf, viral_load_perform_date_cf, viral_load_status_cf, viral_load_count_cf, routine_viral_load_cf, target_cf, PatientGUID) " & _
                "VALUES ('');"
                
             
            Next row
        
        End With
        
                    With ThisWorkbook.Sheets("DataSheetAHD")
                    For row = 4 To .Range("E" & 2).Value + 3

                mssql = "INSERT INTO Tx_Curr_AHD_LineList (Region,Woreda, Facility, HMISCode, ReportYear, ReportMonth, Sex, Weight, Age, PatientGUID, Height, HIV_Confirmed_Date, ARTStartDate, MonthsOnART, FollowUpDate, WHOStage, CD4Count, ARTDoseDays, ARVRegimen, FollowupStatus, AdheranceLevel, IsPregnant, FpMethodUsed, CrAg, NutritionalStatus, FunctionalStatus, No_OI, Zoster, Bacterial_Pneumonia, Extra_Pulmonary_TB, Oesophageal_Candidiasis, Vaginal_Candidiasis, Mouth_Ulcer, Chronic_Diarrhea, Acute_Diarrhea, CNS_Toxoplasmosis, Cryptococcal_Meningitis, Kaposi_Sarcoma, Cervical_Cancer, Pulmonary_TB, Oral_Candidiasis, Pneumocystis_Pneumonia, NonHodgkins_Lymphoma, Genital_Ulcer, OI_Other, Med1, Med2, CotrimoxazoleStartDate, cortimoxazole_stop_date, Fluconazole_Start_Date, Fluconazole_End_Date, TPT_Type, inhprophylaxis_started_date, InhprophylaxisCompletedDate, " & _
                        "TPT_DoseDaysNumberALT, TPT_DoseDaysNumberINH, TPT_Dispensed_Dose, TPT_SideEffect, TPT_Adherence, tb_screened, tb_screening_result, TB_Diagnostic_Result, LF_LAM_result, Gene_Xpert_result, Smear_Microscopy_Result, Additional_TB_Diagnostic_Test_Result, Active_TB, ActiveTBTreatmentStartDate, ActiveTBTreatmentCompletedDate, ActiveTBTreatmentDiscontinuedDate, Viral_Load_Perform_Date, Viral_Load_Status, Viral_Load_count, VL_Sent_Date, Viral_Load_Ref_Date, CCA_Screened, DSD_Category, AHD) " & _
                        "VALUES ('');"
             