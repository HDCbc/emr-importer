INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference)
     SELECT 'entry', ue.id, ees.state, ees.effective_date, ees.emr_reference
       FROM etl.entry_state as ees
       JOIN universal.entry as ue
         ON ue.emr_id = ees.emr_entry_id
        AND ue.emr_table = ees.source_table;

    ANALYZE universal.state;
