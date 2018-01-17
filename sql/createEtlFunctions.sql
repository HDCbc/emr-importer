
CREATE OR REPLACE FUNCTION etl.sync_clinic(
    IN p_table_name text,
    OUT p_updated integer,
    OUT p_inserted integer)
  RETURNS record AS
$BODY$
BEGIN

  EXECUTE FORMAT('
    INSERT INTO universal.clinic (name, hdc_reference, emr_id, emr_reference)
         SELECT ec.name, ec.hdc_reference, ec.emr_clinic_id, ec.emr_reference
           FROM %s as ec;', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.clinic;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_entry(
  IN p_table_name text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.entry (patient_id, emr_table, emr_id)
         SELECT p.id, e.source_table, e.emr_id
           FROM %s as e
           JOIN universal.patient as p
             ON p.emr_id = e.emr_patient_id', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.entry;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_entry_attribute(
  IN p_table_name text,
  IN p_source_table text,
  IN p_attribute_id numeric,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.entry_attribute (entry_id, attribute_id, code_system, code_value, text_value, date_value, boolean_value, numeric_value, emr_id, emr_reference, emr_effective_date, hdc_effective_date)
         SELECT ue.id, ea.attribute_id, ea.code_system, ea.code_value, ea.text_value, ea.date_value, ea.boolean_value, ea.numeric_value, ea.emr_id, ea.emr_reference, ea.effective_date, now()
           FROM %s as ea
           JOIN universal.entry as ue
             ON ue.emr_id = ea.emr_entry_id
            AND ue.emr_table = ea.source_table', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  -- ANALYZE universal.entry_attribute;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_entry_state(
  IN p_table_name text,
  IN p_source_table text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference)
    SELECT ''entry'', ue.id, ees.state, ees.effective_date, ees.emr_reference
      FROM %s as ees
      JOIN universal.entry as ue
        ON ue.emr_id = ees.emr_entry_id
       AND ue.emr_table = ees.source_table', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  -- ANALYZE universal.state;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_patient(
  IN p_table_name text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.patient (clinic_id, emr_id, emr_reference)
         SELECT uc.id, ep.emr_patient_id, ep.emr_reference
           FROM %s as ep
           JOIN universal.clinic as uc
             ON uc.emr_id = ep.emr_clinic_id', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.patient;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_patient_state(
  IN p_table_name text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.state (record_type, record_id, state, effective_date, emr_reference)
         SELECT ''patient'', ue.id, eps.state, eps.effective_date, eps.emr_reference
           FROM %s as eps
           JOIN universal.patient as up
             ON up.emr_id = eps.emr_patient_id;', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.state;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_patient_practitioner(
  IN p_table_name text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.patient_practitioner (patient_id, practitioner_id, emr_id, emr_reference)
    SELECT up.id, ur.id, epp.emr_patient_practitioner_id, epp.emr_reference
      FROM %s as epp
      JOIN universal.patient as up
        ON up.emr_id = epp.emr_patient_id
      JOIN universal.practitioner as ur
        ON ur.emr_id = epp.emr_practitioner_id', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.patient_practitioner;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl.sync_practitioner(
  IN p_table_name text,
  OUT p_updated integer,
  OUT p_inserted integer)
RETURNS record AS
$BODY$
BEGIN

  EXECUTE format('
    INSERT INTO universal.practitioner (clinic_id, name, identifier, identifier_type, emr_id, emr_reference)
    SELECT uc.id, ep.name, ep.identifier, ep.identifier_type, ep.emr_practitioner_id, ep.emr_reference
      FROM %s as ep
      JOIN universal.clinic as uc
        ON uc.emr_id = ep.emr_clinic_id;', p_table_name);

  GET DIAGNOSTICS p_inserted = ROW_COUNT;

  ANALYZE universal.practitioner;

END;
$BODY$
LANGUAGE plpgsql VOLATILE;
