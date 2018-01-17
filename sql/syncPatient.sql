INSERT INTO universal.patient (clinic_id, emr_id, emr_reference)
     SELECT uc.id, ep.emr_patient_id, ep.emr_reference
       FROM etl.patient as ep
       JOIN universal.clinic as uc
         ON uc.emr_id = ep.emr_clinic_id;

    ANALYZE universal.patient;
