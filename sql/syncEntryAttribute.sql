alter table universal.entry_attribute set unlogged;

ALTER TABLE universal.entry_attribute DROP CONSTRAINT entry_attribute_pkey;
ALTER TABLE universal.entry_attribute DROP CONSTRAINT entry_attribute_attribute_id_fkey;
ALTER TABLE universal.entry_attribute DROP CONSTRAINT entry_attribute_entry_id_fkey;

DROP INDEX universal.idx_entry_attribute_attribute_id;
DROP INDEX universal.idx_entry_attribute_code_system;
DROP INDEX universal.idx_entry_attribute_code_value;
DROP INDEX universal.idx_entry_attribute_date_value;
DROP INDEX universal.idx_entry_attribute_entry_id;
DROP INDEX universal.idx_entry_attribute_text_value;

INSERT INTO universal.entry_attribute (entry_id, attribute_id, code_system, code_value, text_value, date_value, boolean_value, numeric_value, emr_id, emr_reference, emr_effective_date, hdc_effective_date)
     SELECT ue.id, ea.attribute_id, ea.code_system, ea.code_value, ea.text_value, ea.date_value, ea.boolean_value, ea.numeric_value, ea.emr_id, ea.emr_reference, ea.effective_date, now()
       FROM etl.entry_attribute as ea
       JOIN universal.entry as ue
         ON ue.emr_id = ea.emr_entry_id
        AND ue.emr_table = ea.source_table;

        ALTER TABLE universal.entry_attribute
          ADD CONSTRAINT entry_attribute_pkey PRIMARY KEY(id);

          ALTER TABLE universal.entry_attribute
            ADD CONSTRAINT entry_attribute_attribute_id_fkey FOREIGN KEY (attribute_id)
                REFERENCES universal.attribute (id) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION;

                ALTER TABLE universal.entry_attribute
                  ADD CONSTRAINT entry_attribute_entry_id_fkey FOREIGN KEY (entry_id)
                      REFERENCES universal.entry (id) MATCH SIMPLE
                      ON UPDATE NO ACTION ON DELETE NO ACTION;

                      CREATE INDEX idx_entry_attribute_attribute_id
                        ON universal.entry_attribute
                        USING btree
                        (attribute_id);

                        CREATE INDEX idx_entry_attribute_code_system
                          ON universal.entry_attribute
                          USING btree
                          (lower(code_system) COLLATE pg_catalog."default");

                          CREATE INDEX idx_entry_attribute_code_value
    ON universal.entry_attribute
    USING btree
    (code_value COLLATE pg_catalog."default");


    CREATE INDEX idx_entry_attribute_date_value
      ON universal.entry_attribute
      USING btree
      (date_value);

      CREATE INDEX idx_entry_attribute_entry_id
        ON universal.entry_attribute
        USING btree
        (entry_id);

        CREATE INDEX idx_entry_attribute_text_value
    ON universal.entry_attribute
    USING btree
    (text_value COLLATE pg_catalog."default");

alter table universal.entry_attribute set logged;

    ANALYZE universal.entry_attribute;
