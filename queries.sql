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
                          AND follow_up_date <= END_DATE -- END_DATE -- endDate
                          AND treatment_end_date >= END_DATE -- END_DATE
                          AND follow_up_status in ('Alive', 'Restart medication') -- alive restart
     ),
     latestDSD_tmp AS (SELECT PatientId,
                          assessment_date                                                                               AS latestDsdDate,
                          encounter_id,
                          dsd_category,
                          ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC , encounter_id DESC ) AS row_num
                   FROM FollowUp
                   WHERE assessment_date IS NOT NULL
                     AND assessment_date <= END_DATE -- END_DATE
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
         left join latestDSD on latestDSD.PatientId = tx_curr.PatientId
;




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

-- Get latest viral load performed dates
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
                                          END                                                                                             AS viral_load_ref_date, -- Q this should be null?
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE follow_up_status IS NOT NULL
                                 AND art_start_date IS NOT NULL
                                 AND viral_load_perform_date <= END_DATE -- '" & eDate & "'
                               GROUP BY client_id, encounter_id),

     latest_follow_up_tmp AS (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= END_DATE -- END_DATE -- endDate
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
                         specimen_sent_to_lab                AS TB_ProphylaxisType,
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
                         dsd_category
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
-- VL Sent Date
     tmp_vl_sent_date AS (select PatientId,
                                 encounter_id,
                                 viral_load_sent_date,
                                 ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          from FollowUp
                          where follow_up_date <= END_DATE),
     vl_sent_date AS (select * from tmp_vl_sent_date where row_num = 1),
-- VL Performed date

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
                                 AND viral_load_perform_date <= END_DATE -- '" & eDate & "'
     ),
     vl_performed_date AS (select * from vl_performed_date_tmp where row_num = 1),
     tx_curr_all AS (SELECT PatientId,
                            follow_up_date                                                                             AS FollowupDate,
                            encounter_id,
                            ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= END_DATE     -- END_DATE -- endDate
                       AND treatment_end_date >= END_DATE -- END_DATE
                       AND follow_up_status in ('Alive', 'Restart medication') -- alive restart
     ),
     tx_curr AS (select * from tx_curr_all where row_num = 1)
SELECT DISTINCT f_case.sex                                         as Sex,
                f_case.Weight                                      as Weight,
                Age as Age,
                uuid                                               as PatientGUID,
                height                                             as Height,
                date_hiv_confirmed,
                art_start_date,
                FLOOR(DATEDIFF(END_DATE,art_start_date)/30.4375) AS MonthsOnART,
                f_case.follow_up_date                              as FollowUpDate,
                current_who_hiv_stage                              as WHO,
                cd4_count  AS CD4,
                art_dose_days                                      AS ARTDoseDays,
                regimen                                            as ARVRegimen,
                follow_up_status,
                adherence                                          AS AdheranceLevel,
                pregnancy_status                                   as IsPregnant,
                method_of_family_planning                          as FpMethodUsed,
                crag                                               as CrAg,
                COALESCE(
                        ns_adult,
                        NSAdolescent,
                        NSLessthanFive
                )                                                  AS NutritionalStatus,
                current_functional_status                          AS FunctionalStatus,
                No_OI,
                Zoster,
                Bacterial_Pneumonia,
                Extra_Pulmonary_TB,
                Oesophageal_Candidiasis,
                Vaginal_Candidiasis,
                Mouth_Ulcer,
                Chronic_Diarrhea,
                Acute_Diarrhea,
                CNS_Toxoplasmosis,
                Cryptococcal_Meningitis,
                Kaposi_Sarcoma,
                Cervical_Cancer,
                Pulmonary_TB,
                Oral_Candidiasis,
                Pneumocystis_Pneumonia,
                NonHodgkins_Lymphoma,
                Genital_Ulcer,
                OI_Other,
#                 otherm.Med1,
#                 otherm.Med2,
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
                f_case.LF_LAM_result,
                f_case.Gene_Xpert_result,
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
                patient_diagnosed_with_active_tuber                   Active_TB,
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
                         cd4_count < 200 THEN 'Yes'
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






