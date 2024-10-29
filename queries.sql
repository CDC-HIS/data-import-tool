
WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id                 AS PatientId,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         assessment_date,
                         treatment_end_date,
                         antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                         sex,
                         current_age                         AS Age,
                         weight_text_                        AS Weight,
                         screening_test_result_tuberculosis  AS TB_SreeningStatus,
                         date_of_last_menstrual_period_lmp_     LMP_Date,
                         anitiretroviral_adherence_level     AS AdherenceLevel,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         person.uuid,
                         diagnosis_date                      AS ActiveTBDiagnoseddate,
                         nutritional_status_of_adult,
                         nutritional_supplements_provided,
                         stages_of_disclosure,
                         date_started_on_tuberculosis_prophy,
                         method_of_family_planning,
                         patient_diagnosed_with_active_tuber as ActiveTBDiagnosed,
                         dsd_category,
                         nutritional_screening_result
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                        JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           JOIN mamba_dim_client_art_follow_up dim_client ON follow_up.client_id = dim_client.client_id
                           JOIN mamba_dim_person person on person.person_id = follow_up.client_id
                           left join analysis_db.mamba_flat_encounter_intake_a mfeia
                                     on dim_client.client_id = mfeia.client_id
                  ),
     -- TX curr
     tx_curr_all AS (SELECT PatientId,
                               follow_up_date                                                                             AS FollowupDate,
                               encounter_id,
                               ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                        FROM FollowUp
                        WHERE follow_up_status IS NOT NULL
                          AND art_start_date IS NOT NULL
                          AND follow_up_date <= END_DATE
                          AND treatment_end_date >= END_DATE
                          AND follow_up_status in ('Alive', 'Restart medication')
     ),
     latestDSD_tmp AS (SELECT PatientId,
                          assessment_date                                                                               AS latestDsdDate,
                          encounter_id,
                          dsd_category,
                          ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC , encounter_id DESC ) AS row_num
                   FROM FollowUp
                   WHERE assessment_date IS NOT NULL
                     AND assessment_date <= END_DATE
     ),

     latestDSD AS (select * from latestDSD_tmp where row_num = 1),
     tx_curr AS ( select * from tx_curr_all where row_num=1)
select sex,
       Weight,
       Age,
       follow_up_date                                        As FollowUpDate,
       FollowupDate                                          as FollowUpDate_GC,
       next_visit_date                                       As Next_visit_DateET,
       next_visit_date                                       As Next_visit_Date_GC,
       regimen as ARVRegimen,
       ARTDoseDays,
       follow_up_status as FollowUpStatus,
       treatment_end_date as ARTDoseEndDate,
       treatment_end_date as ARTDoseEndDate_GC,
       AdherenceLevel,
       art_start_date as ARTStartDate,
       art_start_date as ARTStartDate_GC,
       CASE
                       WHEN method_of_family_planning = 'Intrauterine device' OR
                            method_of_family_planning = 'Vasectomy'
                           OR method_of_family_planning = 'None' THEN 'LongTermFP'
                       WHEN method_of_family_planning = 'Diaphragm' OR
                            method_of_family_planning = 'Implantable contraceptive (unspecified type)' OR
                            method_of_family_planning = 'Oral contraception' OR method_of_family_planning = 'Injectable contraceptives' OR
                            method_of_family_planning = 'Condoms' THEN 'ShortTermFP' END               AS FP_Status,

       TB_SreeningStatus,
       ActiveTBDiagnosed,
       nutritional_screening_result as NutritionalScrenningStatus,
       CASE
           When nutritional_status_of_adult is not null then Case
                                                                 when Age BETWEEN 15 AND 49
                                                                     Then
                                                                     Case sex
                                                                         When 'FEMALE' Then Case pregnancy_status
                                                                                                when 'No'
                                                                                                    then 'Female:NotPregnant'
                                                                                                when 'Yes'
                                                                                                    then 'Female:Pregnant'
                                                                                                else 'Female:NotPregnant' End
                                                                         ELSE sex end
                                                                 else sex END
           ELSE sex END                                      As SexForNutrition,
       nutritional_supplements_provided as TherapeuticFoodProvided,
       uuid as PatientGUID,
       pregnancy_status as IsPregnant,
       breast_feeding_status as BreastFeeding,
       LMP_Date,
       LMP_Date as LMP_Date_GC,
       FLOOR(DATEDIFF(END_DATE,art_start_date)/30.4375) AS MonthsOnART,
       stages_of_disclosure as ChildDisclosueStatus,
       FollowUp.PatientId,
       latestDSD.dsd_category as dsd_category
from FollowUp
         inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
         left join latestDSD on latestDSD.PatientId = tx_curr.PatientId;

WITH FollowUp AS (SELECT follow_up.client_id,
                         follow_up.encounter_id,
                         date_viral_load_results_received AS viral_load_perform_date,
                         viral_load_received_,
                         follow_up_status,
                         follow_up_date_followup_         AS follow_up_date,
                         art_antiretroviral_start_date       art_start_date,
                         viral_load_test_status,
                         hiv_viral_load                   AS viral_load_count,
                         COALESCE(
                                 at_3436_weeks_of_gestation,
                                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                 every_six_months_until_mtct_ends,
                                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                 three_months_after_delivery,
                                 at_the_first_antenatal_care_visit,
                                 annual_viral_load_test,
                                 second_viral_load_test_at_12_months_post_art,
                                 first_viral_load_test_at_6_months_or_longer_post_art,
                                 first_viral_load_test_at_3_months_or_longer_post_art
                         )                                AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure
                         )                                AS targeted_viral_load_test_indication,
                         viral_load_test_indication,
                         pregnancy_status,
                         currently_breastfeeding_child    AS breastfeeding_status,
                         antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
                         regimen,
                         next_visit_date,
                         treatment_end_date,
                         date_of_event                       date_hiv_confirmed,
                         sex,
                         current_age,
                         patient_name,
                         weight_text_                     as weight,
                         mrn,
                         uan,
                         uuid
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           JOIN mamba_dim_client_art_follow_up dim_client
                                ON follow_up.client_id = dim_client.client_id
                           JOIN mamba_dim_person person on person.person_id = follow_up.client_id),
     vl_performed_date_tmp AS (SELECT FollowUp.encounter_id,
                                      FollowUp.client_id,
                                      FollowUp.viral_load_perform_date,
                                      FollowUp.viral_load_test_status,
                                      CASE
                                          WHEN viral_load_count > 0 THEN CAST(viral_load_count AS DECIMAL(12, 0))
                                          END                                                                                             AS viral_load_count,
                                      CASE
                                          WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                              THEN FollowUp.viral_load_perform_date
                                          END                                                                                             AS viral_load_ref_date,
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE follow_up_status IS NOT NULL
                                 AND art_start_date IS NOT NULL
                                 AND viral_load_perform_date <= END_DATE
                               GROUP BY client_id, encounter_id),
     latest_follow_up_tmp AS (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= END_DATE
     ),
     latest_follow_up AS (select * from latest_follow_up_tmp where row_num = 1),
     vl_performed_date as (select * from vl_performed_date_tmp where row_num = 1),
     vl_test_received AS (SELECT current_age,
                                 arv_dispensed_dose,
                                 treatment_end_date,
                                 art_start_date,
                                 regimen,
                                 breastfeeding_status,
                                 date_hiv_confirmed,
                                 follow_up_status,
                                 follow_up_date,
                                 pregnancy_status,
                                 next_visit_date,
                                 FollowUp.client_id,
                                 uuid,
                                 CASE
                                     WHEN pregnancy_status = 'Yes' THEN 'Yes'
                                     WHEN breastfeeding_status = 'Yes' THEN 'Yes'
                                     ELSE 'No' END AS PMTCT_ART,
                                 vlperfdate.viral_load_count,
                                 vlperfdate.viral_load_perform_date,
                                 vlperfdate.viral_load_ref_date,
                                 vlperfdate.viral_load_test_status,
                                 sex,
                                 FollowUp.weight,
                                 vlperfdate.routine_viral_load_test_indication,
                                 vlperfdate.targeted_viral_load_test_indication
                          FROM FollowUp
                                   INNER JOIN latest_follow_up ON latest_follow_up.encounter_id = FollowUp.encounter_id
                                   LEFT JOIN vl_performed_date as vlperfdate
                                             ON vlperfdate.client_id = FollowUp.client_id)
select sex,
       weight                              as Weight,
       current_age                         as age,
       date_hiv_confirmed,
       art_start_date                      as art_start_date,
       follow_up_date                      as FollowUpDate,
       pregnancy_status                    as IsPregnant,
       breastfeeding_status                as BreastFeeding,
       regimen                             as ARVDispendsedDose,
       regimen                             as ARVRegimenLine,
       arv_dispensed_dose                  as art_dose,
       next_visit_date,
       follow_up_status,
       treatment_end_date                  as art_dose_End,
       viral_load_perform_date,
       viral_load_test_status,
       viral_load_count,
       viral_load_ref_date,
       CONCAT(IFNULL(routine_viral_load_test_indication, ''), ' ', IFNULL(targeted_viral_load_test_indication, '')) AS ReasonForVLTest,
       PMTCT_ART,
       uuid                                as PatientGUID
from vl_test_received
where viral_load_perform_date is not null
  And viral_load_perform_date >= DATE_ADD(END_DATE, INTERVAL -365 DAY)
  and viral_load_perform_date <= END_DATE; 

WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id                 AS PatientId,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                         date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                         tb_prophylaxis_type                AS TB_ProphylaxisType,
                         tpt_dispensed_dose_in_days_alternat AS TPT_DoseDaysNumberALT, -- ???
                         tpt_side_effects                    AS TPT_SideEffect,
                         diagnostic_test                     AS DiagnosticTest,
                         tb_diagnostic_test_result           AS DiagnosticTestResult,
                         lf_lam_result                       AS LF_LAM_result,
                         gene_xpert_result                   AS Gene_Xpert_result,
                         tuberculosis_drug_treatment_start_d AS activetbtreatmentStartDate,
                         tpt_dispensed_dose_in_days_inh_     AS TPT_DoseDaysNumberINH,
                         was_the_patient_screened_for_tuberc AS tb_screened,
                         tb_screening_date                   AS tb_screening,
                         Adherence                           AS TPT_Adherance,
                         date_discontinued_tuberculosis_prop AS inhprophylaxisdiscontinuedDate,
                         date_active_tbrx_completed          AS ActiveTBTreatmentCompletedDate,
                         date_active_tbrx_dc                 AS activetbtreatmentDisContinuedDate,
                         cervical_cancer_screening_status    AS CCS_ScreenDoneYes,
                         date_of_reported_hiv_viral_load     AS viral_load_sent_date,
                         date_viral_load_results_received    AS viral_load_perform_date,
                         viral_load_test_status,
                         hiv_viral_load                      AS viral_load_count,
                         viral_load_test_indication,
                         treatment_end_date,
                         sex,
                         weight_text_                        AS Weight,
                         age,
                         uuid,
                         height,
                         date_of_event                       AS date_hiv_confirmed,
                         current_who_hiv_stage,
                         cd4_count,
                         antiretroviral_art_dispensed_dose_i AS art_dose_days,
                         regimen,
                         adherence,
                         pregnancy_status,
                         method_of_family_planning,
                         crag,
                         cotrimoxazole_prophylaxis_start_dat,
                         cotrimoxazole_prophylaxis_stop_date,
                         current_functional_status,
                         patient_diagnosed_with_active_tuber,
                         fluconazole_start_date              AS Fluconazole_Start_Date,
                         weight_for_age_status               AS NSLessthanFive,
                         nutritional_status_of_older_child_a AS NSAdolescent,
                         nutritional_status_of_adult         AS ns_adult,
                         no_opportunistic_illness            AS No_OI,
                         herpes_zoster                       AS Zoster,
                         bacterial_pneumonia                 AS Bacterial_Pneumonia,
                         extra_pulmonary_tuberculosis_tb     AS Extra_Pulmonary_TB,
                         candidiasis_of_the_esophagus        AS Oesophageal_Candidiasis,
                         candidiasis_vaginal                 AS Vaginal_Candidiasis,
                         mouth_ulcer                         AS Mouth_Ulcer,
                         diarrhea_chronic                    AS Chronic_Diarrhea,
                         acute_diarrhea                      AS Acute_Diarrhea,
                         toxoplasmosis                       AS CNS_Toxoplasmosis,
                         meningitis_cryptococcal             AS Cryptococcal_Meningitis,
                         kaposi_sarcoma_oral                 AS Kaposi_Sarcoma,
                         suspected_cervical_cancer           AS Cervical_Cancer,
                         pulmonary_tuberculosis_tb           AS Pulmonary_TB,
                         candidiasis_oral                    AS Oral_Candidiasis,
                         pneumocystis_carinii_pneumonia_pcp  AS Pneumocystis_Pneumonia,
                         malignant_lymphoma_nonhodgkins      AS NonHodgkins_Lymphoma,
                         female_genital_ulcer_disease        AS Genital_Ulcer,
                         other_opportunistic_illnesses       AS OI_Other,
                         fluconazole_stop_date as Fluconazole_End_Date,
                         nutritional_screening_result,
                         dsd_category,
                         other_medications_med_1 Med1,
                         other_medications_med2 Med2
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           JOIN mamba_dim_client_art_follow_up dim_client ON follow_up.client_id = dim_client.client_id
                           JOIN mamba_dim_person person on person.person_id = follow_up.client_id),
     tpt_start AS (SELECT patientid, MAX(inhprophylaxis_started_date) AS inhprophylaxis_started_date
                   FROM FollowUp
                   WHERE inhprophylaxis_started_date IS NOT NULL
                   GROUP BY patientid),
     tpt_completed AS (SELECT patientid, Max(InhprophylaxisCompletedDate) AS InhprophylaxisCompletedDate
                       FROM FollowUp
                       WHERE InhprophylaxisCompletedDate IS NOT NULL
                       GROUP BY patientid),
     tpt_type AS (SELECT patientid, Max(TB_ProphylaxisType) AS TB_ProphylaxisType
                  FROM FollowUp
                  WHERE TB_ProphylaxisType IS NOT NULL
                  GROUP BY patientid),
     tpt_dose_ALT AS (SELECT patientid, Max(TPT_DoseDaysNumberALT) AS TPT_DoseDaysNumberALT
                      FROM FollowUp
                      WHERE TPT_DoseDaysNumberALT IS NOT NULL
                      GROUP BY patientid),

     tpt_dose_INH AS (SELECT patientid,
                             Max(TPT_DoseDaysNumberINH) AS TPT_DoseDaysNumberINH
                      FROM FollowUp
                      WHERE TPT_DoseDaysNumberINH IS NOT NULL

                      GROUP BY patientid),
     tpt_side_effect AS (SELECT patientid, Max(TPT_SideEffect) AS TPT_SideEffect
                         FROM FollowUp
                         WHERE TPT_SideEffect IS NOT NULL
                         GROUP BY patientid),
     tb_diagnostic_test AS (SELECT patientid, Max(DiagnosticTest) AS TB_Diagnostic_Test
                            FROM FollowUp
                            WHERE DiagnosticTest IS NOT NULL
                            GROUP BY patientid),
     tb_diagnostic_result AS (SELECT patientid, Max(DiagnosticTestResult) AS TB_Diagnostic_Result
                              FROM FollowUp
                              WHERE DiagnosticTestResult IS NOT NULL
                              GROUP BY patientid),
     tb_LF_LAM_result AS (SELECT patientid, Max(LF_LAM_result) AS LF_LAM_result
                          FROM FollowUp
                          WHERE LF_LAM_result IS NOT NULL
                          GROUP BY patientid),
     tb_Gene_Xpert_result AS (SELECT patientid, Max(Gene_Xpert_result) AS Gene_Xpert_result
                              FROM FollowUp
                              WHERE Gene_Xpert_result IS NOT NULL
                              GROUP BY patientid),
     tpt_screened AS (SELECT patientid, Max(tb_screened) AS TB_Screened
                      FROM FollowUp
                      WHERE tb_screened IS NOT NULL
                      GROUP BY patientid),
     tpt_screening AS (SELECT patientid, Max(tb_screening) AS TB_Screening_Result
                       FROM FollowUp
                       WHERE tb_screening IS NOT NULL
                       GROUP BY patientid),
     tpt_adherence AS (SELECT patientid, Max(TPT_Adherance) AS TPT_Adherence
                       FROM FollowUp
                       WHERE TPT_Adherance IS NOT NULL
                       GROUP BY patientid),
     ActiveTBTreatmentStarted AS (SELECT patientid, Max(activetbtreatmentStartDate) AS ActiveTBTreatmentStartDate
                                  FROM FollowUp
                                  WHERE  activetbtreatmentStartDate IS NOT NULL
                                  GROUP BY patientid),
     TBTreatmentCompleted AS (SELECT patientid, Max(ActiveTBTreatmentCompletedDate) AS ActiveTBTreatmentCompletedDate
                              FROM FollowUp
                              WHERE ActiveTBTreatmentCompletedDate IS NOT NULL
                              GROUP BY patientid),
     TBTreatmentDiscontinued AS (SELECT patientid,
                                        Max(activetbtreatmentDisContinuedDate) AS ActiveTBTreatmentDiscontinuedDate
                                 FROM FollowUp
                                 WHERE
                                    activetbtreatmentDisContinuedDate IS NOT NULL
                                 GROUP BY patientid),
     cca_screened_tmp AS (SELECT DISTINCT patientid, CCS_ScreenDoneYes AS CCA_Screened,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.PatientId ORDER BY follow_up_date DESC, FollowUp.encounter_id DESC) AS row_num
                      FROM FollowUp
                      where CCS_ScreenDoneYes IS NOT NULL),
    cca_screened AS ( select * from cca_screened_tmp where row_num=1),
     tmp_vl_sent_date AS (select PatientId,
                                 encounter_id,
                                 viral_load_sent_date,
                                 ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          from FollowUp
                          where follow_up_date <= END_DATE),
     vl_sent_date AS (select * from tmp_vl_sent_date where row_num = 1),

     vl_performed_date_tmp AS (SELECT FollowUp.encounter_id,
                                      FollowUp.PatientId,
                                      FollowUp.viral_load_perform_date,
                                      FollowUp.viral_load_test_status,
                                     FollowUp.viral_load_count             AS viral_load_count,
                                      CASE
                                          WHEN vl_sent_date.viral_load_sent_date IS NOT NULL
                                              THEN vl_sent_date.viral_load_sent_date
                                          WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                              THEN FollowUp.viral_load_perform_date
                                          ELSE NULL END                                                                                                     AS viral_load_ref_date,
                                      ROW_NUMBER() OVER (PARTITION BY FollowUp.PatientId ORDER BY viral_load_perform_date DESC, FollowUp.encounter_id DESC) AS row_num
                               FROM FollowUp
                                        LEFT JOIN vl_sent_date ON FollowUp.PatientId = vl_sent_date.PatientId
                               WHERE follow_up_status IS NOT NULL
                                 AND art_start_date IS NOT NULL
                                 AND viral_load_perform_date <= END_DATE
     ),
     vl_performed_date AS (select * from vl_performed_date_tmp where row_num = 1),
     tx_curr_all AS (SELECT PatientId,
                            follow_up_date                                                                             AS FollowupDate,
                            encounter_id,
                            ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= END_DATE
                       AND treatment_end_date >= END_DATE
                       AND follow_up_status in ('Alive', 'Restart medication')
     ),
     tx_curr AS (select * from tx_curr_all where row_num = 1)
SELECT DISTINCT f_case.sex                                         as Sex,
                f_case.Weight                                      as Weight,
                f_case.Age,
                f_case.uuid                                               as PatientGUID,
                f_case.height                                             as Height,
                f_case.date_hiv_confirmed,
                f_case.art_start_date,
                FLOOR(DATEDIFF(END_DATE,f_case.art_start_date)/30.4375) AS MonthsOnART,
                f_case.follow_up_date                              as FollowUpDate,
                f_case.current_who_hiv_stage                              as WHO,
                CASE
                    WHEN f_case.cd4_count REGEXP '^[0-9]+(\.[0-9]+)?$' > 0 THEN CAST(f_case.cd4_count AS DECIMAL(12, 2))
                    ELSE NULL END                                  AS CD4,
                f_case.art_dose_days                                      AS ARTDoseDays,
                f_case.regimen                                            as ARVRegimen,
                f_case.follow_up_status,
                tpt_adherence.tpt_adherence                        AS AdheranceLevel,
                f_case.pregnancy_status                                   as IsPregnant,
                f_case.method_of_family_planning                          as FpMethodUsed,
                f_case.crag                                               as CrAg,
                COALESCE(
                        f_case.ns_adult,
                        f_case.NSAdolescent,
                        f_case.NSLessthanFive
                )                                                  AS NutritionalStatus,
                f_case.current_functional_status                          AS FunctionalStatus,
                f_case.No_OI,
                f_case.Zoster,
                f_case.Bacterial_Pneumonia,
                f_case.Extra_Pulmonary_TB,
                f_case.Oesophageal_Candidiasis,
                f_case.Vaginal_Candidiasis,
                f_case.Mouth_Ulcer,
                f_case.Chronic_Diarrhea,
                f_case.Acute_Diarrhea,
                f_case.CNS_Toxoplasmosis,
                f_case.Cryptococcal_Meningitis,
                f_case.Kaposi_Sarcoma,
                f_case.Cervical_Cancer,
                f_case.Pulmonary_TB,
                f_case.Oral_Candidiasis,
                f_case.Pneumocystis_Pneumonia,
                f_case.NonHodgkins_Lymphoma,
                f_case.Genital_Ulcer,
                f_case.OI_Other,
                f_case.Med1,
                f_case.Med2,
                f_case.cotrimoxazole_prophylaxis_start_dat         as CotrimoxazoleStartDate,
                f_case.cotrimoxazole_prophylaxis_stop_date         as cortimoxazole_stop_date,
                f_case.Fluconazole_Start_Date                      as Fluconazole_Start_Date,
                f_case.Fluconazole_End_Date as Fluconazole_End_Date,
                tpt_type.TB_ProphylaxisType                        AS TPT_Type,
                tpt_start.inhprophylaxis_started_date              as inhprophylaxis_started_date,
                tpt_completed.InhprophylaxisCompletedDate          as InhprophylaxisCompletedDate,
                tpt_dose_ALT.TPT_DoseDaysNumberALT                 as TPT_DoseDaysNumberALT,
                tpt_dose_INH.TPT_DoseDaysNumberINH                 as TPT_DoseDaysNumberINH,
                COALESCE(tpt_dose_INH.TPT_DoseDaysNumberINH,tpt_dose_ALT.TPT_DoseDaysNumberALT) AS TPT_Dispensed_Dose,
                tpt_side_effect.TPT_SideEffect                     as TPT_SideEffect,
                tpt_adherence.TPT_Adherence                        AS TPT_Adherence,
                tpt_screened.TB_Screened                           AS tb_screened,
                tpt_screening.TB_Screening_Result                  AS tb_screening_result,
                tb_diagnostic_result.TB_Diagnostic_Result          AS TB_Diagnostic_Result,
                tb_LF_LAM_result.LF_LAM_result,
                tb_Gene_Xpert_result.Gene_Xpert_result,
                CASE
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                        THEN 'Positive'
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                        THEN 'Negative'
                    ELSE '' END                                    AS Smear_Microscopy_Result,
                CASE
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                        THEN 'Positive'
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                        THEN 'Negative'
                    ELSE '' END                                    AS Additional_TB_Diagnostic_Test_Result,
                f_case.patient_diagnosed_with_active_tuber                   Active_TB,
                ActiveTBTreatmentStarted.ActiveTBTreatmentStartDate,
                TBTreatmentCompleted.ActiveTBTreatmentCompletedDate,
                TBTreatmentDiscontinued.ActiveTBTreatmentDiscontinuedDate,
                vlperfdate.viral_load_perform_date,
                vlperfdate.viral_load_test_status                  as viral_load_status,
                vlperfdate.viral_load_count,
                vlsentdate.viral_load_sent_date                    as VL_Sent_Date,
                vlperfdate.viral_load_ref_date,
                cca_screened.CCA_Screened                          AS CCA_Screened,
                f_case.dsd_category AS DSD_Category,
                CASE
                    WHEN f_case.age < 5 THEN 'Yes'
                    WHEN f_case.age >= 5 AND f_case.cd4_count IS NOT NULL AND
                         f_case.cd4_count < 200 THEN 'Yes'
                    WHEN f_case.age >= 5 AND f_case.current_who_hiv_stage IS NOT NULL AND
                         (f_case.current_who_hiv_stage = 2 Or f_case.current_who_hiv_stage = 6 Or
                          f_case.current_who_hiv_stage = 7) THEN 'Yes'
                    WHEN (f_case.age >= 5 AND f_case.current_who_hiv_stage IS NOT NULL AND
                          f_case.current_who_hiv_stage = 3) THEN 'Yes'
                    ELSE 'No' END                                  AS AHD,
                f_case.encounter_id                                as Id,
                f_case.PatientId

FROM FollowUp AS f_case
         INNER JOIN tx_curr ON f_case.encounter_id = tx_curr.encounter_id
         LEFT JOIN vl_performed_date AS vlperfdate ON vlperfdate.PatientId = f_case.PatientId
         LEFT JOIN vl_sent_date AS vlsentdate ON vlsentdate.PatientId = f_case.PatientId

         LEFT JOIN tpt_start ON tpt_start.patientid = f_case.PatientId
         LEFT JOIN tpt_completed ON tpt_completed.patientid = f_case.PatientId
         LEFT JOIN tpt_type ON tpt_type.patientid = f_case.PatientId
         LEFT JOIN tpt_dose_ALT ON tpt_dose_ALT.patientid = f_case.PatientId
         LEFT JOIN tpt_dose_INH ON tpt_dose_INH.patientid = f_case.PatientId
         LEFT JOIN tpt_side_effect ON tpt_side_effect.patientid = f_case.PatientId
         LEFT JOIN
     tpt_screened ON tpt_screened.patientid = f_case.PatientId
         LEFT JOIN tpt_screening ON tpt_screening.patientid = f_case.PatientId
         LEFT JOIN tpt_adherence ON tpt_adherence.patientid = f_case.PatientId
         LEFT JOIN tb_diagnostic_result ON tb_diagnostic_result.patientid = f_case.PatientId
         LEFT JOIN tb_diagnostic_test ON tb_diagnostic_test.patientid = f_case.PatientId
         LEFT JOIN tb_LF_LAM_result ON tb_LF_LAM_result.patientid = f_case.PatientId
         LEFT JOIN tb_Gene_Xpert_result ON tb_Gene_Xpert_result.patientid = f_case.PatientId
         LEFT JOIN ActiveTBTreatmentStarted ON ActiveTBTreatmentStarted.patientid = f_case.PatientId
         LEFT JOIN TBTreatmentCompleted ON TBTreatmentCompleted.patientid = f_case.PatientId
         LEFT JOIN TBTreatmentDiscontinued ON TBTreatmentDiscontinued.patientid = f_case.PatientId
         LEFT JOIN cca_screened ON cca_screened.patientid = f_case.PatientId;

WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                          as follow_up_date,
                         follow_up_status,
                         art_antiretroviral_start_date                     as art_start_date,
                         regimen_change                                    as switch,
                         date_of_reported_hiv_viral_load                      viral_load_sent_date,
                         date_viral_load_results_received                     viral_load_performed_date,
                         viral_load_test_status,
                         hiv_viral_load                                    as viral_load_count,
                         COALESCE(
                                 at_3436_weeks_of_gestation,
                                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                 every_six_months_until_mtct_ends,
                                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                 three_months_after_delivery,
                                 at_the_first_antenatal_care_visit,
                                 annual_viral_load_test,
                                 second_viral_load_test_at_12_months_post_art,
                                 first_viral_load_test_at_6_months_or_longer_post_art,
                                 first_viral_load_test_at_3_months_or_longer_post_art
                         )                                                 AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure
                         )                                                 AS targeted_viral_load_test_indication,
                         date_third_enhanced_adherence_counseling_provided as eac_3,
                         date_second_enhanced_adherence_counseling_provided   eac_2,
                         date_first_enhanced_adherence_counseling_provided    eac_1,
                         age,
                         weight_text_                                      as weight,
                         date_of_event                                     as hiv_confirmed_date,
                         pregnancy_status,
                         antiretroviral_art_dispensed_dose_i                  dispensed_dose,
                         regimen,
                         next_visit_date,
                         treatment_end_date                                   art_dose_end_date,
                         uuid,
                         gender

                  from mamba_flat_encounter_follow_up follow_up
                           join mamba_flat_encounter_follow_up_1 follow_up_1
                                on follow_up.encounter_id = follow_up_1.encounter_id
                           join mamba_flat_encounter_follow_up_2 follow_up_2
                                on follow_up.encounter_id = follow_up_2.encounter_id
                           join mamba_flat_encounter_follow_up_3 follow_up_3
                                on follow_up.encounter_id = follow_up_3.encounter_id
                           join mamba_dim_person person on follow_up.client_id = person.person_id),


     tmp_switch_sub_date AS (SELECT encounter_id,
                                    client_id,
                                    follow_up_date                                                                            as switch_date,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date, FollowUp.encounter_id) AS row_num
                             FROM FollowUp
                             WHERE follow_up_date <= END_DATE
                               and switch is not null),
     switch_sub_date AS (select * from tmp_switch_sub_date where row_num = 1),

     tmp_vl_performed_date_1 AS (SELECT encounter_id,
                                        client_id,
                                        viral_load_performed_date,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_performed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                 FROM FollowUp
                                 where viral_load_performed_date is not null
                                   AND follow_up_date <= END_DATE),
     tmp_vl_performed_date_1_dedup AS (select * from tmp_vl_performed_date_1 where row_num = 1),

     tmp_vl_sent_date AS (SELECT FollowUp.client_id,
                                 viral_load_sent_date                                                                                                          AS VL_Sent_Date,
                                 ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_sent_date DESC , FollowUp.encounter_id DESC ) AS row_num
                          FROM FollowUp
                                   Inner Join tmp_vl_performed_date_1_dedup
                                              ON tmp_vl_performed_date_1_dedup.client_id = FollowUp.client_id
                          WHERE FollowUp.follow_up_date <= END_DATE
                            and viral_load_sent_date is not null),
     vl_sent_date AS (select *
                      from tmp_vl_sent_date
                      where row_num = 1),

     vl_performed_date AS (SELECT FollowUp.encounter_id,
                                  FollowUp.client_id,
                                  FollowUp.viral_load_performed_date,
                                  FollowUp.viral_load_test_status,
                                  viral_load_count,
                                  CASE
                                      WHEN vl_sent_date.VL_Sent_Date IS NOT NULL
                                          THEN vl_sent_date.VL_Sent_Date
                                      WHEN FollowUp.viral_load_performed_date IS NOT NULL
                                          THEN FollowUp.viral_load_performed_date
                                      Else NULL END AS viral_load_ref_date,
                                  routine_viral_load_test_indication     AS routine_viral_load,
                                  targeted_viral_load_test_indication    AS target
                           FROM FollowUp
                                    INNER JOIN tmp_vl_performed_date_1_dedup
                                               ON FollowUp.encounter_id = tmp_vl_performed_date_1_dedup.encounter_id
                                    LEFT JOIN vl_sent_date ON FollowUp.client_id = vl_sent_date.client_id),


     tmp_vl_performed_date_cf AS (SELECT encounter_id,
                                         client_id,
                                         viral_load_performed_date                                                                                                          AS viral_load_perform_date,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_performed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                  FROM FollowUp
                                  where follow_up_status Is Not Null
                                    And targeted_viral_load_test_indication Is Not Null
                                    AND follow_up_date <= END_DATE),

     tmp_vl_sent_date_cf AS (SELECT FollowUp.client_id,
                                    viral_load_sent_date                                                                                                          AS VL_Sent_Date,
                                    ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_sent_date DESC , FollowUp.encounter_id DESC ) AS row_num
                             FROM FollowUp
                                      Inner Join tmp_vl_performed_date_cf
                                                 ON tmp_vl_performed_date_cf.client_id = FollowUp.client_id
                             WHERE FollowUp.follow_up_date <= END_DATE
                               and viral_load_sent_date is not null),
     vl_sent_date_cf AS (select *
                         from tmp_vl_sent_date_cf
                         where row_num = 1),


     tmp_vl_performed_date_cf_2 AS (select * from tmp_vl_performed_date_cf where row_num = 1),
     tmp_vl_performed_date_cf_3 AS (SELECT FollowUp.encounter_id,
                                           FollowUp.client_id,
                                           FollowUp.viral_load_performed_date,
                                           FollowUp.viral_load_test_status,
                                           FollowUp.viral_load_count              AS viral_load_count,
                                           CASE
                                               WHEN vl_sent_date_cf.VL_Sent_Date IS NOT NULL
                                                   THEN vl_sent_date_cf.VL_Sent_Date
                                               WHEN FollowUp.viral_load_performed_date IS NOT NULL
                                                   THEN FollowUp.viral_load_performed_date
                                               Else NULL END AS viral_load_ref_date,
                                           routine_viral_load_test_indication     AS routine_viral_load_cf,

                                           targeted_viral_load_test_indication    AS target_cf
                                    FROM FollowUp
                                             INNER JOIN tmp_vl_performed_date_cf_2
                                                        ON FollowUp.encounter_id = tmp_vl_performed_date_cf_2.encounter_id
                                             LEFT JOIN vl_sent_date_cf ON FollowUp.client_id = vl_sent_date_cf.client_id),
     tmp_vl_perf_date_eac_1 AS (SELECT FollowUp.client_id,
                                       follow_up_date                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_1 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND FollowUp.follow_up_date <= END_DATE),
     tmp_vl_perf_date_eac_2 AS (SELECT FollowUp.client_id,
                                       follow_up_date                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_2 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND FollowUp.follow_up_date <= END_DATE),
     tmp_vl_perf_date_eac_3 AS (SELECT FollowUp.client_id,
                                       follow_up_date                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) as row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_3 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND FollowUp.follow_up_date <= END_DATE),
     vl_perf_date_eac_1 AS (select * from tmp_vl_perf_date_eac_1 where row_num = 1),
     vl_perf_date_eac_2 AS (select * from tmp_vl_perf_date_eac_2 where row_num = 1),
     vl_perf_date_eac_3 AS (select * from tmp_vl_perf_date_eac_3 where row_num = 1),
     tmp_latest_follow_up AS (SELECT client_id,
                                     FollowUp.encounter_id,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) as row_num
                              FROM FollowUp
                              where follow_up_status Is Not Null
                                AND follow_up_date <= END_DATE),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),

     hvl as (SELECT f_case.uuid                             as PatientGUID,
                    age                                     AS age,
                    f_case.gender                           as Sex,
                    f_case.encounter_id                     as Id,
                    f_case.client_id                        as PatientId,
                    f_case.weight,
                    f_case.hiv_confirmed_date               as date_hiv_confirmed,
                    f_case.art_start_date,
                    f_case.follow_up_date                   as FollowUpDate,
                    f_case.pregnancy_status                 as IsPregnant,
                    f_case.dispensed_dose                   as ARVDispendsedDose,
                    f_case.regimen                          as art_dose,
                    f_case.next_visit_date,
                    f_case.follow_up_status,
                    f_case.art_dose_end_date                as art_dose_End,
                    vlperfdate.viral_load_performed_date    as viral_load_perform_date,
                    vlperfdate.viral_load_test_status       as viral_load_status,
                    vlperfdate.viral_load_count,
                    vlsentdate.VL_Sent_Date,
                    vlperfdate.viral_load_ref_date,
                    sub_switch_date.switch_date             as SwitchDate,
                    date_eac1.Date_EAC_Provided             as date_eac_provided_1,
                    date_eac2.Date_EAC_Provided             as date_eac_provided_2,
                    date_eac3.Date_EAC_Provided             as date_eac_provided_3,
                    vlsentdate_cf.VL_Sent_Date              as viral_load_sent_date_cf,
                    vlperfdate_cf.viral_load_performed_date as viral_load_perform_date_cf,
                    vlperfdate_cf.viral_load_test_status    as viral_load_status_cf,
                    vlperfdate_cf.viral_load_count          as viral_load_count_cf,
                    vlperfdate.routine_viral_load,
                    vlperfdate.target,
                    vlperfdate_cf.routine_viral_load_cf,
                    vlperfdate_cf.target_cf
             FROM FollowUp AS f_case
                      INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
                      LEFT JOIN vl_performed_date as vlperfdate ON vlperfdate.client_id = f_case.client_id
                      LEFT JOIN tmp_vl_performed_date_cf_3 as vlperfdate_cf
                                ON vlperfdate_cf.client_id = f_case.client_id
                      Left join vl_sent_date as vlsentdate ON vlsentdate.client_id = f_case.client_id
                      Left join vl_sent_date_cf as vlsentdate_cf ON vlsentdate_cf.client_id = f_case.client_id
                      Left join vl_perf_date_eac_1 as date_eac1 ON date_eac1.client_id = f_case.client_id
                      Left join vl_perf_date_eac_2 as date_eac2 ON date_eac2.client_id = f_case.client_id
                      Left join vl_perf_date_eac_3 as date_eac3 ON date_eac3.client_id = f_case.client_id
                      Left join switch_sub_date as sub_switch_date ON sub_switch_date.client_id = f_case.client_id)
select Sex,
       weight      as Weight,
       age,
       date_hiv_confirmed,
       art_start_date,
       FollowUpDate,
       IsPregnant,
       ARVDispendsedDose,
       art_dose,
       next_visit_date,
       follow_up_status,
       art_dose_End,
       viral_load_perform_date,
       viral_load_status,
       viral_load_count,
       vl_sent_date   viral_load_sent_date,
       viral_load_ref_date,
       routine_viral_load,
       hvl.target,
       SwitchDate  as date_regimen_change,
       date_eac_provided_1,
       date_eac_provided_2,
       date_eac_provided_3,
       viral_load_sent_date_cf,
       viral_load_perform_date_cf,
       viral_load_status_cf,
       viral_load_count_cf,
       routine_viral_load_cf,
       target_cf,
       PatientGUID as PatientGUID
from hvl
where hvl.follow_up_status in ('Alive', 'Restart medication')
  and hvl.art_dose_End >= END_DATE
  AND art_start_date <= END_DATE; 

WITH FollowUp AS (SELECT follow_up.client_id,
                         follow_up.encounter_id,
                         date_viral_load_results_received AS viral_load_perform_date,
                         viral_load_received_,
                         follow_up_status,
                         follow_up_date_followup_         AS follow_up_date,
                         art_antiretroviral_start_date       art_start_date,
                         viral_load_test_status,
                         hiv_viral_load                   AS viral_load_count,
                         COALESCE(
                                 at_3436_weeks_of_gestation,
                                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                 every_six_months_until_mtct_ends,
                                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                 three_months_after_delivery,
                                 at_the_first_antenatal_care_visit,
                                 annual_viral_load_test,
                                 second_viral_load_test_at_12_months_post_art,
                                 first_viral_load_test_at_6_months_or_longer_post_art,
                                 first_viral_load_test_at_3_months_or_longer_post_art
                         )                                AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure
                         )                                AS targeted_viral_load_test_indication,
                         viral_load_test_indication,
                         pregnancy_status,
                         currently_breastfeeding_child    AS breastfeeding_status,
                         antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
                         regimen,
                         next_visit_date,
                         treatment_end_date,
                         date_of_event                       date_hiv_confirmed,
                         sex,
                         current_age,
                         patient_name,
                         weight_text_                     as weight,
                         mrn,
                         uan,
                         uuid,
                         date_of_reported_hiv_viral_load  as viral_load_sent_date,
                         regimen_change,
                         mobile_no
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           JOIN mamba_dim_client_art_follow_up dim_client
                                ON follow_up.client_id = dim_client.client_id
                           JOIN mamba_dim_person person on person.person_id = follow_up.client_id),


     tmp_all_art_follow_ups as (SELECT encounter_id,
                                       client_id,
                                       follow_up_status,
                                       follow_up_date                                                                             AS FollowUpDate,
                                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                FROM FollowUp
                                WHERE follow_up_date <= END_DATE
                                  and follow_up_date >= START_DATE),

     all_art_follow_ups as (select * from tmp_all_art_follow_ups where row_num = 1),

     tmp_vl_sent_date as (SELECT encounter_id,
                                 client_id,
                                 viral_load_sent_date                                                                             AS VL_Sent_Date,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE viral_load_sent_date is not null
                            and viral_load_sent_date <= END_DATE
                            and viral_load_sent_date >= START_DATE),
     vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),

     tmp_switch_sub_date as (SELECT encounter_id,
                                    client_id,
                                    follow_up_date                                                                             AS FollowUpDate,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                             FROM FollowUp
                             WHERE follow_up_date <= END_DATE
                               and follow_up_date >= START_DATE
                               and regimen_change is not null),
     switch_sub_date as (select * from tmp_switch_sub_date where row_num = 1),

     tmp_vl_performed_date_1 as (SELECT encounter_id,
                                        client_id,
                                        viral_load_perform_date                                                                             AS viral_load_perform_date,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL
                                   AND (
                                     (viral_load_perform_date IS NOT NULL AND
                                      viral_load_perform_date <= END_DATE
                                         AND viral_load_perform_date
                                          >= START_DATE)
                                         OR
                                     viral_load_perform_date IS NULL
                                     )),
     tmp_vl_performed_date_2 as (select * from tmp_vl_performed_date_1 where row_num = 1),

     tmp_vl_performed_date_3 as (SELECT FollowUp.encounter_id,
                                        FollowUp.client_id,
                                        case
                                            when FollowUp.viral_load_perform_date < vl_sent_date.VL_Sent_Date then null
                                            else FollowUp.viral_load_perform_date end as viral_load_perform_date,
                                        case
                                            when FollowUp.viral_load_perform_date < vl_sent_date.VL_Sent_Date then null
                                            else FollowUp.viral_load_test_status end  as viral_load_status,
                                        CASE
                                            WHEN FollowUp.viral_load_count > 0 AND
                                                 FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date
                                                THEN CAST(FollowUp.viral_load_count AS DECIMAL(12, 2))
                                            ELSE NULL END                             AS viral_load_count,
                                        CASE
                                            WHEN
                                                viral_load_test_status IS NULL AND
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date
                                                THEN
                                                NULL
                                            WHEN FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                 (viral_load_test_status LIKE 'Det%'
                                                     OR viral_load_test_status LIKE 'Uns%'
                                                     OR viral_load_test_status LIKE 'High VL%'
                                                     OR viral_load_test_status LIKE 'Low Level Viremia%')
                                                THEN
                                                'U'
                                            WHEN FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                 (viral_load_test_status LIKE 'Su%'
                                                     OR viral_load_test_status LIKE 'Undet%')
                                                THEN
                                                'S'
                                            WHEN
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                (ISNULL(viral_load_count) > CAST(50 AS float)
                                                    )
                                                THEN
                                                'U'
                                            WHEN
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                (ISNULL(viral_load_count) <= CAST(50 AS float)
                                                    )
                                                THEN
                                                'S'
                                            ELSE
                                                NULL
                                            END                                       AS viral_load_status_inferred,
                                        CASE
                                            WHEN vl_sent_date.VL_Sent_Date IS NOT NULL
                                                THEN vl_sent_date.VL_Sent_Date
                                            WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                                THEN FollowUp.viral_load_perform_date
                                            ELSE NULL END                             AS viral_load_ref_date,
                                        FollowUp.routine_viral_load_test_indication
                                 FROM FollowUp
                                          INNER JOIN tmp_vl_performed_date_2
                                                     ON FollowUp.encounter_id = tmp_vl_performed_date_2.encounter_id
                                          LEFT JOIN vl_sent_date
                                                    ON FollowUp.client_id = vl_sent_date.client_id),
     tmp_latest_alive_restart as (SELECT encounter_id,
                                         client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status in ('Alive', 'Restart medication')
                                    AND follow_up_date <= END_DATE
                                    AND follow_up_date >= START_DATE),
     latest_alive_restart as (select * from tmp_latest_alive_restart where row_num = 1),
     vl_eligibility as (SELECT f_case.art_start_date                         as art_start_date,
                               f_case.breastfeeding_status                   as BreastFeeding,
                               f_case.date_hiv_confirmed                     as date_hiv_confirmed,
                               sub_switch_date.FollowupDate                  as date_regimen_change,
                               all_art_follow_ups.follow_up_status           as follow_up_status,
                               f_case.follow_up_date                         as FollowUpDate,
                               patient_name                                  as FullName,
                               f_case.pregnancy_status                       as IsPregnant,
                               mobile_no                                     as MobilePhoneNumber,
                               mrn                                           as MRN,
                               uuid                                          as PatientGUID,
                               f_case.client_id                              as PatientId,
                               sex                                           as Sex,
                               vlperfdate.viral_load_count                   as viral_load_count,
                               vlperfdate.viral_load_perform_date            as viral_load_perform_date,
                               vlsentdate.VL_Sent_Date                       as viral_load_sent_date,
                               vlperfdate.viral_load_status                  as viral_load_status,
                               current_age,
                               CASE
                                   WHEN vlsentdate.VL_Sent_Date IS NOT NULL
                                       THEN vlsentdate.VL_Sent_Date
                                   WHEN vlperfdate.viral_load_perform_date IS NOT NULL
                                       THEN vlperfdate.viral_load_perform_date
                                   ELSE NULL END                             AS viral_load_ref_date,
                               sub_switch_date.FollowupDate SwitchDate,
                               vlperfdate.viral_load_status_inferred,

                               vlperfdate.routine_viral_load_test_indication as viral_load_indication,

                               CASE

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.follow_up_status = 'Restart medication')
                                       THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND sub_switch_date.FollowupDate IS NOt NULL
                                           )
                                       THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.pregnancy_status = 'Yes'
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) > 90)
                                       THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) <= 180)
                                       THEN NULL


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) > 180)
                                       THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < f_case.follow_up_date)
                                           AND (f_case.follow_up_status = 'Restart medication')
                                       THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < sub_switch_date.FollowupDate
                                           AND sub_switch_date.FollowupDate IS NOT NULL
                                           )
                                       THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_status_inferred = 'U')
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                           AND vlperfdate.routine_viral_load_test_indication in
                                               ('First viral load test at 6 months or longer post ART',
                                                'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                           AND vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                           AND vlperfdate.routine_viral_load_test_indication not in
                                               ('First viral load test at 6 months or longer post ART',
                                                'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL)
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)

                                   ELSE '12-31-9999' End                     AS eligiblityDate,

                               CASE

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.follow_up_status = 'Restart medication')
                                       THEN 'client restarted ART'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND sub_switch_date.FollowupDate IS NOt NULL
                                           )
                                       THEN 'Regimen Change'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.pregnancy_status = 'Yes'
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) > 90)
                                       THEN 'First VL for Pregnant'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) <= 180)
                                       THEN 'N/A'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, END_DATE) > 180)
                                       THEN 'First VL'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < f_case.follow_up_date)
                                           AND (f_case.follow_up_status = 'Restart medication')
                                       THEN 'client restarted ART'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < sub_switch_date.FollowupDate
                                           AND sub_switch_date.FollowupDate IS NOT NULL
                                           )
                                       THEN 'Regimen Change'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_status_inferred = 'U')
                                       THEN 'Repeat/Confirmatory Viral Load test'

                                   WHEN
                                       (vlperfdate.viral_load_status_inferred IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes'))
                                       THEN 'Pregnant/Breastfeeding and needs retesting'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL)
                                       THEN 'Annual Viral Load Test'

                                   ELSE 'Unassigned' End                     AS vl_status_final

                        FROM FollowUp AS f_case
                                 INNER JOIN latest_alive_restart
                                            ON f_case.encounter_id = latest_alive_restart.encounter_id

                                 LEFT JOIN tmp_vl_performed_date_3 as vlperfdate
                                           ON vlperfdate.client_id = f_case.client_id

                                 Left join vl_sent_date as vlsentdate
                                           ON vlsentdate.client_id = f_case.client_id

                                 Left join switch_sub_date as sub_switch_date
                                           ON sub_switch_date.client_id = f_case.client_id


                                 Left join all_art_follow_ups on f_case.client_id = all_art_follow_ups.client_id

                        where all_art_follow_ups.follow_up_status in ('Alive', 'Restart Medication'))
select case

           when t.vl_status_final = 'N/A' THEN 'Not Applicable'
           when t.eligiblityDate <= END_DATE THEN 'Eligible for Viral Load'
           when t.eligiblityDate > END_DATE THEN 'Viral Load Done'
           when t.art_start_date is NULL and t.follow_up_status is null THEN 'Not Started ART'
           end as viral_load_status_compare,
       t.*
from vl_eligibility t; 

WITH FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         date_of_event                       as hiv_confirmed_date,
                         art_antiretroviral_start_date       as art_start_date,
                         follow_up_date_followup_            as followup_date,
                         weight_text_                        as weight_in_kg,
                         pregnancy_status,
                         regimen,
                         antiretroviral_art_dispensed_dose_i as art_dose_days,
                         follow_up_status,
                         anitiretroviral_adherence_level,
                         next_visit_date,
                         dsd_category,
                         date_started_on_tuberculosis_prophy as tpt_start_date,
                         date_completed_tuberculosis_prophyl as tpt_completed_date,
                         date_discontinued_tuberculosis_prop as tpt_discontinued_date,
                         date_viral_load_results_received    as viral_load_performed_date,
                         viral_load_test_status,
                         treatment_end_date                  as art_end_date,
                         current_who_hiv_stage,
                         cd4_count,
                         cotrimoxazole_prophylaxis_start_dat,
                         cotrimoxazole_prophylaxis_stop_date,
                         patient_diagnosed_with_active_tuber as active_tb_dx,
                         diagnosis_date,
                         tuberculosis_drug_treatment_start_d,
                         date_active_tbrx_completed,
                         tb_prophylaxis_type                 AS TB_ProphylaxisType,
                         tb_prophylaxis_type_alternate_      AS TB_ProphylaxisTypeALT,
                         tpt_followup_6h_                       tpt_follow_up_inh,
                         date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                         date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                         why_eligible_reason_,
                         tb_diagnostic_test_result              tb_specimen_type,
                         gender,
                         age,
                         uuid,
                         fluconazole_start_date              AS Fluconazole_Start_Date,
                         fluconazole_stop_date               as Fluconazole_End_Date,
                         transfer_in
                  FROM mamba_flat_encounter_follow_up follow_up
                           join mamba_flat_encounter_follow_up_1 follow_up_1
                                on follow_up.encounter_id = follow_up_1.encounter_id
                           join mamba_flat_encounter_follow_up_2 follow_up_2
                                on follow_up.encounter_id = follow_up_2.encounter_id
                           join mamba_flat_encounter_follow_up_3 follow_up_3
                                on follow_up.encounter_id = follow_up_3.encounter_id
                           join mamba_dim_person person on follow_up.client_id = person.person_id),
     tmp_tpt_type as (SELECT encounter_id,
                             client_id,
                             TB_ProphylaxisType                                                                                                     AS TptType,
                             TB_ProphylaxisTypeAlt                                                                                                  AS TptTypeAlt,
                             tpt_follow_up_inh                                                                                                      As TPTFollowup,
                             followup_date                                                                                                          AS FollowupDate,
                             ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC , FollowUp.encounter_id DESC ) AS row_num
                      FROM FollowUp
                      where followup_date <= END_DATE
                        and (TB_ProphylaxisType is not null OR TB_ProphylaxisTypeAlt is not null OR
                             tpt_follow_up_inh is not null)),

     tmp_tpt_start as (select encounter_id,
                              client_id,
                              inhprophylaxis_started_date                                                                                                          as inhprophylaxis_started_date,
                              ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.inhprophylaxis_started_date DESC , FollowUp.encounter_id DESC ) AS row_num
                       from FollowUp
                       where inhprophylaxis_started_date is not null
                         and followup_date <= END_DATE),
     tmp_tpt_completed as (select encounter_id,
                                  client_id,
                                  InhprophylaxisCompletedDate                                                                                                          as InhprophylaxisCompletedDate,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.InhprophylaxisCompletedDate DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where InhprophylaxisCompletedDate is not null
                             and followup_date <= END_DATE),

     tmp_latest_follow_up as (SELECT encounter_id,
                                     client_id,
                                     followup_date                                                                                                         AS FollowupDate,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC, FollowUp.encounter_id DESC ) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND followup_date <= END_DATE),
     tpt_type as (select * from tmp_tpt_type where row_num = 1),
     tpt_start as (select * from tmp_tpt_start where row_num = 1),
     tpt_completed as (select * from tmp_tpt_completed where row_num = 1),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
     tmp_tpt as (SELECT f_case.encounter_id,
                        f_case.client_id,
                        f_case.gender,
                        f_case.weight_in_kg,
                        f_case.age,
                        f_case.hiv_confirmed_date,
                        f_case.art_start_date,
                        f_case.followup_date,
                        f_case.why_eligible_reason_,
                        art_dose_days                       as artdosecode,
                        f_case.next_visit_date,
                        f_case.follow_up_status,
                        f_case.follow_up_status             as statuscode,
                        f_case.art_end_date,
                        f_case.current_who_hiv_stage        AS WHOStage,
                        cd4_count                              AdultCD4Count,
                        cd4_count                              ChildCD4Count,
                        cotrimoxazole_prophylaxis_start_dat As CPT_StartDate,
                        cotrimoxazole_prophylaxis_start_dat As CPT_StartDate_GC,
                        cotrimoxazole_prophylaxis_stop_date As CPT_StopDate,
                        cotrimoxazole_prophylaxis_stop_date As CPT_StopDate_GC,
                        tb_specimen_type                    AS TB_SpecimenType,
                        active_tb_dx                        As ActiveTBDiagnosed,
                        diagnosis_date                      As ActiveTBDignosedDate,
                        diagnosis_date                      As ActiveTBDignosedDate_GC,
                        tuberculosis_drug_treatment_start_d As TBTx_StartDate,
                        tuberculosis_drug_treatment_start_d As TBTx_StartDate_GC,
                        date_active_tbrx_completed          As TBTx_CompletedDate,
                        date_active_tbrx_completed          As TBTx_CompletedDate_GC,
                        uuid                                as PatientGUID
                 FROM FollowUp AS f_case
                          INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id)

select tmp_tpt.gender                            as Sex,
       tmp_tpt.weight_in_kg,
       tmp_tpt.age                               AS Age,
       tpt_start.inhprophylaxis_started_date     As TPT_Started_Date,
       tpt_completed.InhprophylaxisCompletedDate As TPT_Completed_Date,
       tpt_type.TptType,
       tpt_type.TptTypeAlt,
       CASE
           WHEN tpt_type.TptType = '6H' THEN 'INH'
           WHEN tpt_type.TptType = '3HP' THEN '3HP'
           ELSE '' END                           AS TPT_TypeChar,

       tmp_tpt.hiv_confirmed_date,
       tmp_tpt.art_start_date,
       tmp_tpt.followup_date,
#        #temp3.transferin                                                                    As Transfer_In,
       tmp_tpt.artdosecode                       As ARTDoseDays,
       tmp_tpt.next_visit_date,
       tmp_tpt.follow_up_status,
       tmp_tpt.statuscode                        As FollowupStatusChar,
       tmp_tpt.art_end_date                      As ARTDoseEndDate,
       tmp_tpt.PatientGUID,
       tmp_tpt.WHOStage,
       tmp_tpt.AdultCD4Count,
       tmp_tpt.ChildCD4Count,
       tmp_tpt.CPT_StartDate,
       tmp_tpt.CPT_StartDate_GC,
       tmp_tpt.CPT_StopDate,
       tmp_tpt.CPT_StopDate_GC,
       tmp_tpt.TB_SpecimenType,
       tmp_tpt.ActiveTBDiagnosed,
       tmp_tpt.ActiveTBDignosedDate,
       tmp_tpt.ActiveTBDignosedDate_GC,
       tmp_tpt.TBTx_StartDate,
       tmp_tpt.TBTx_StartDate_GC,
       tmp_tpt.TBTx_CompletedDate,
       tmp_tpt.TBTx_CompletedDate_GC,
       FollowUp.fluconazole_start_date           As FluconazoleStartDate,
       FollowUp.fluconazole_start_date           As FluconazoleStartDate_GC,
       FollowUp.Fluconazole_End_Date             As FluconazoleEndDate,
       FollowUp.Fluconazole_End_Date             As FluconazoleEndDate_GC
FROM FollowUp
         inner join tmp_tpt on tmp_tpt.encounter_id = FollowUp.encounter_id
         Left join tpt_start on tmp_tpt.client_id = tpt_start.client_id
         Left join tpt_completed on tmp_tpt.client_id = tpt_completed.client_id
         Left join tpt_type on tmp_tpt.client_id = tpt_type.client_id
where tmp_tpt.art_end_date >= END_DATE
  AND tmp_tpt.follow_up_status in ('Alive', 'Restart medication')
  AND tmp_tpt.art_start_date <= END_DATE; 