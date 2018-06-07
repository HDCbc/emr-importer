INSERT INTO audit.import_log (
  finish_time,
  attribute_count,
  clinic_count,
  entry_count,
  entry_attribute_count,
  patient_count,
  patient_practitioner_count,
  practitioner_count,
  state_count
)
VALUES (
  CURRENT_TIMESTAMP,
  (SELECT COUNT(*) FROM universal.attribute),
  (SELECT COUNT(*) FROM universal.clinic),
  (SELECT COUNT(*) FROM universal.entry),
  (SELECT COUNT(*) FROM universal.entry_attribute),
  (SELECT COUNT(*) FROM universal.patient),
  (SELECT COUNT(*) FROM universal.patient_practitioner),
  (SELECT COUNT(*) FROM universal.practitioner),
  (SELECT COUNT(*) FROM universal.state)
);
