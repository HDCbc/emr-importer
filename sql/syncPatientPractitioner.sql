INSERT INTO universal.patient_practitioner (patient_id, practitioner_id, emr_id, emr_reference)
     SELECT up.id, ur.id, epp.emr_patient_practitioner_id, epp.emr_reference
       FROM etl.patient_practitioner as epp
       JOIN universal.patient as up
         ON up.emr_id = epp.emr_patient_id
       JOIN universal.practitioner as ur
         ON ur.emr_id = epp.emr_practitioner_id;

     ANALYZE universal.patient_practitioner;
