-- DROP INDEX universal.idx_entry_emr;
--ALTER TABLE universal.entry DROP CONSTRAINT entry_patient_id_fkey;

 INSERT INTO universal.entry (patient_id, emr_table, emr_id)
      SELECT p.id, e.source_table, e.emr_id
        FROM etl.entry as e
        JOIN universal.patient as p
          ON p.emr_id = e.emr_patient_id;

--  CREATE INDEX idx_entry_emr ON universal.entry USING btree (emr_table, emr_id);

     ANALYZE universal.entry;
