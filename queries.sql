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
                          AND follow_up_date <= END_DATE -- '2024-01-01' -- endDate
                          AND treatment_end_date >= END_DATE -- '2024-01-01'
                          AND follow_up_status in ('Alive', 'Restart medication') -- alive restart
     ),
     latestDSD_tmp AS (SELECT PatientId,
                          assessment_date                                                                               AS latestDsdDate,
                          encounter_id,
                          dsd_category,
                          ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC , encounter_id DESC ) AS row_num
                   FROM FollowUp
                   WHERE assessment_date IS NOT NULL
                     AND assessment_date <= END_DATE -- '2024-01-01'
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
       FLOOR(DATEDIFF('2024-09-30',art_start_date)/30.4375) AS MonthsOnART,
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
                         routine_viral_load_test_indication,
                         targeted_viral_load_test_indication,
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
                                      viral_load_test_indication,
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
                                AND follow_up_date <= END_DATE -- '2024-01-01' -- endDate
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
                                 vlperfdate.viral_load_test_indication,
                                 vlperfdate.viral_load_count,
                                 vlperfdate.viral_load_perform_date,
                                 vlperfdate.viral_load_ref_date,
                                 vlperfdate.viral_load_test_status,
                                 sex,
                                 FollowUp.weight,
                                 routine_viral_load_test_indication,
                                 targeted_viral_load_test_indication

                          FROM FollowUp
                                   INNER JOIN latest_follow_up ON latest_follow_up.encounter_id = FollowUp.encounter_id
                                   LEFT JOIN vl_performed_date as vlperfdate
                                             ON vlperfdate.client_id = FollowUp.client_id)
select sex,
       weight                                                                     as Weight,
       current_age                                                                as age,
       date_hiv_confirmed,
       art_start_date                                                             as art_start_date,
       follow_up_date                                                             as FollowUpDate,
       pregnancy_status                                                           as IsPregnant,
       breastfeeding_status                                                       as BreastFeeding,
       regimen                                                                    as ARVDispendsedDose,
       regimen                                                                    as ARVRegimenLine,
       arv_dispensed_dose                                                         as art_dose,
       next_visit_date,
       follow_up_status,
       treatment_end_date                                                         as art_dose_End,
       viral_load_perform_date,
       viral_load_test_status,
       viral_load_count,
       viral_load_ref_date,
       (routine_viral_load_test_indication + targeted_viral_load_test_indication) as ReasonForVLTest,
       viral_load_test_indication,
       PMTCT_ART,
       uuid                                                                       as PatientGUID
from vl_test_received
where viral_load_perform_date is not null
  And viral_load_perform_date >= DATE_ADD(END_DATE, INTERVAL -365 DAY)
  and viral_load_perform_date <= END_DATE;










