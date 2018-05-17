INSERT INTO universal.clinic (name, hdc_reference, emr_id, emr_reference, emr)
     SELECT ec.name, ec.hdc_reference, ec.emr_clinic_id, ec.emr_reference, ec.emr
       FROM etl.clinic as ec;

    ANALYZE universal.clinic;
