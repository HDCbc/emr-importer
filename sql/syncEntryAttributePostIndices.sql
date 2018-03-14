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
