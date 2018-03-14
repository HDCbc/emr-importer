INSERT INTO universal.entry_attribute (entry_id, attribute_id, code_system, code_value, text_value, date_value, boolean_value, numeric_value, emr_id, emr_reference, emr_effective_date, hdc_effective_date)
     SELECT ue.id, ea.attribute_id, ea.code_system, ea.code_value, ea.text_value, ea.date_value, ea.boolean_value, ea.numeric_value, ea.emr_id, ea.emr_reference, ea.effective_date, now()
       FROM etl.entry_attribute as ea
       JOIN universal.entry as ue
         ON ue.emr_id = ea.emr_entry_id
        AND ue.emr_table = ea.source_table;
