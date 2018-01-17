DROP SCHEMA IF EXISTS etl CASCADE;
CREATE SCHEMA etl;

CREATE UNLOGGED TABLE IF NOT EXISTS etl.clinic (name text, hdc_reference text, emr_clinic_id text, emr_reference text);
ALTER TABLE etl.clinic SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.practitioner (emr_clinic_id text, name text, identifier text, identifier_type text, emr_practitioner_id text, emr_reference text);
ALTER TABLE etl.practitioner SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.patient_practitioner (emr_patient_id text, emr_practitioner_id text, emr_patient_practitioner_id text, emr_reference text);
ALTER TABLE etl.patient_practitioner SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.patient (emr_clinic_id text, emr_patient_id text, emr_reference text);
ALTER TABLE etl.patient SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.patient_state (emr_patient_id text, state text, effective_date timestamp with time zone, emr_reference text);
ALTER TABLE etl.patient_state SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.entry_attribute (source_table text, attribute_id numeric(6,3), emr_entry_id text, code_system text, code_value text, text_value text, date_value date, boolean_value boolean, numeric_value numeric(18,6), emr_id text, effective_date date, emr_reference text);
ALTER TABLE etl.entry_attribute SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.entry_state (source_table text, emr_entry_id text, state text, effective_date timestamp with time zone, emr_reference text);
ALTER TABLE etl.entry_state SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS etl.entry (source_table text, emr_id text, emr_patient_id text);
ALTER TABLE etl.entry SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
