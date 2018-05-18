ALTER TABLE universal.entry_attribute DROP CONSTRAINT IF EXISTS entry_attribute_pkey;
-- This constraint is no longer created. Leaving the drop for endpoints that may still have it.
ALTER TABLE universal.entry_attribute DROP CONSTRAINT IF EXISTS entry_attribute_attribute_id_fkey;
ALTER TABLE universal.entry_attribute DROP CONSTRAINT IF EXISTS entry_attribute_entry_id_fkey;
