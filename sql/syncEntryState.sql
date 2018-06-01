INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference, emr_id)
     SELECT 'entry', ue.id, ees.state, ees.effective_date, ees.emr_reference, ees.emr_id
       FROM etl.entry_state as ees
       JOIN universal.entry as ue
         ON ue.emr_id = ees.emr_entry_id
        AND ue.emr_table = ees.source_table
        AND ees.entry_type_id = ue.entry_type_id;

    ANALYZE universal.state;
