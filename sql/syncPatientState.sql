INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference)
     SELECT 'patient', up.id, eps.state, eps.effective_date, eps.emr_reference
       FROM etl.patient_state as eps
       JOIN universal.patient as up
         ON up.emr_id = eps.emr_patient_id;

    ANALYZE universal.state;
