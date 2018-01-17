INSERT INTO universal.practitioner (clinic_id, name, identifier, identifier_type, emr_id, emr_reference)
     SELECT uc.id, ep.name, ep.identifier, ep.identifier_type, ep.emr_practitioner_id, ep.emr_reference
       FROM etl.practitioner as ep
       JOIN universal.clinic as uc
         ON uc.emr_id = ep.emr_clinic_id;

    ANALYZE universal.practitioner;
