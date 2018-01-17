    ANALYZE;

INSERT INTO universal.clinic (name, hdc_reference, emr_id, emr_reference)
     SELECT ec.name, ec.hdc_reference, ec.emr_clinic_id, ec.emr_reference
       FROM etl.clinic as ec;

    ANALYZE universal.clinic;

INSERT INTO universal.patient (clinic_id, emr_id, emr_reference)
     SELECT uc.id, ep.emr_patient_id, ep.emr_reference
       FROM etl.patient as ep
       JOIN universal.clinic as uc
         ON uc.emr_id = ep.emr_clinic_id;

    ANALYZE universal.patient;

INSERT INTO universal.practitioner (clinic_id, name, identifier, identifier_type, emr_id, emr_reference)
     SELECT uc.id, ep.name, ep.identifier, ep.identifier_type, ep.emr_practitioner_id, ep.emr_reference
       FROM etl.practitioner as ep
       JOIN universal.clinic as uc
         ON uc.emr_id = ep.emr_clinic_id;

    ANALYZE universal.practitioner;

INSERT INTO universal.patient_practitioner (patient_id, practitioner_id, emr_id, emr_reference)
     SELECT up.id, ur.id, epp.emr_patient_practitioner_id, epp.emr_reference
       FROM etl.patient_practitioner as epp
       JOIN universal.patient as up
         ON up.emr_id = epp.emr_patient_id
       JOIN universal.practitioner as ur
         ON ur.emr_id = epp.emr_practitioner_id;

     ANALYZE universal.patient_practitioner;

-- DROP INDEX universal.idx_entry_emr;
--ALTER TABLE universal.entry DROP CONSTRAINT entry_patient_id_fkey;

 INSERT INTO universal.entry (patient_id, emr_table, emr_id)
      SELECT p.id, e.source_table, e.emr_id
        FROM etl.entry as e
        JOIN universal.patient as p
          ON p.emr_id = e.emr_patient_id;

--  CREATE INDEX idx_entry_emr ON universal.entry USING btree (emr_table, emr_id);

     ANALYZE universal.entry;

 INSERT INTO universal.entry_attribute (entry_id, attribute_id, code_system, code_value, text_value, date_value, boolean_value, numeric_value, emr_id, emr_reference, emr_effective_date, hdc_effective_date)
      SELECT ue.id, ea.attribute_id, ea.code_system, ea.code_value, ea.text_value, ea.date_value, ea.boolean_value, ea.numeric_value, ea.emr_id, ea.emr_reference, ea.effective_date, now()
        FROM etl.entry_attribute as ea
        JOIN universal.entry as ue
          ON ue.emr_id = ea.emr_entry_id
         AND ue.emr_table = ea.source_table;

     ANALYZE universal.entry_attribute;

 INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference)
      SELECT 'entry', ue.id, ees.state, ees.effective_date, ees.emr_reference
        FROM etl.entry_state as ees
        JOIN universal.entry as ue
          ON ue.emr_id = ees.emr_entry_id
         AND ue.emr_table = ees.source_table;

     ANALYZE universal.state;
