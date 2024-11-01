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
                          AND follow_up_date <= REPORT_END_DATE
                          AND treatment_end_date >= REPORT_END_DATE
                          AND follow_up_status in ('Alive', 'Restart medication')
     ),
     latestDSD_tmp AS (SELECT PatientId,
                          assessment_date                                                                               AS latestDsdDate,
                          encounter_id,
                          dsd_category,
                          ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY assessment_date DESC , encounter_id DESC ) AS row_num
                   FROM FollowUp
                   WHERE assessment_date IS NOT NULL
                     AND assessment_date <= REPORT_END_DATE
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
       FLOOR(DATEDIFF(REPORT_END_DATE,art_start_date)/30.4375) AS MonthsOnART,
       stages_of_disclosure as ChildDisclosueStatus,
       FollowUp.PatientId,
       latestDSD.dsd_category as dsd_category
from FollowUp
         inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
         left join latestDSD on latestDSD.PatientId = tx_curr.PatientId;