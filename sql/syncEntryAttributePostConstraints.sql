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
