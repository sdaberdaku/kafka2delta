avro_schema_str = """
{
    "type": "record",
    "name": "Value",
    "namespace": "tenant.public.test_assetlog",
    "fields": [
        {"name": "id", "type": {"type": "int", "connect.default": 0}, "default": 0},
        {"name": "created_at", "type": ["null", {"type": "string", "connect.version": 1, "connect.name": "io.debezium.time.ZonedTimestamp"}], "default": null},
        {"name": "updated_at", "type": ["null", {"type": "string", "connect.version": 1, "connect.name": "io.debezium.time.ZonedTimestamp"}], "default": null},
        {"name": "date", "type": {"type": "int", "connect.version": 1, "connect.name": "org.apache.kafka.connect.data.Date", "logicalType": "date"}},
        {"name": "loan_connection_name", "type": ["null", "string"], "default": null},
        {"name": "property_address", "type": ["null", "string"], "default": null},
        {"name": "town", "type": ["null", "string"], "default": null},
        {"name": "county", "type": ["null", "string"], "default": null},
        {"name": "postcode", "type": ["null", "string"], "default": null},
        {"name": "loan_type", "type": ["null", "string"], "default": null},
        {"name": "currency_abbreviation", "type": ["null", "string"], "default": null},
        {"name": "gross_loan_amount", "type": ["null", "string"], "default": null},
        {"name": "allocation", "type": ["null", "string"], "default": null},
        {"name": "repayment_type", "type": ["null", "string"], "default": null},
        {"name": "repayment_schedule", "type": ["null", "string"], "default": null},
        {"name": "security_charge", "type": ["null", "string"], "default": null},
        {"name": "drawdown_date", "type": ["null", {"type": "int", "connect.version": 1, "connect.name": "org.apache.kafka.connect.data.Date", "logicalType": "date"}], "default": null},
        {"name": "redemption_date", "type": ["null", {"type": "int", "connect.version": 1, "connect.name": "org.apache.kafka.connect.data.Date", "logicalType": "date"}], "default": null},
        {"name": "asset_id", "type": ["null", "int"], "default": null},
        {"name": "__deleted", "type": ["null", "string"], "default": null},
        {"name": "__timestamp", "type": ["null", "long"], "default": null},
        {"name": "__log_sequence_number", "type": ["null", "long"], "default": null}
    ],
    "connect.name": "tenant.public.test_assetlog.Value"
}
"""

b64_encoded_values = [
    "AAAABhdAAjYyMDI0LTA3LTExVDA3OjEzOjMwLjQ1NjE0MVoCNjIwMjQtMDctMTFUMDc6MTM6MzAuNDU2MTUzWpa3AgAAAAAAAAAAAhAwLjAwMDAwMAAAAAACircCAhQCCmZhbHNlApyY9Z6mZAKAgOb+gZEe",
    "AAAABhc6AjYyMDIzLTEyLTA0VDExOjMwOjIxLjgzNTkzNFoCNjIwMjMtMTItMDRUMTE6MzA6MjEuODM1OTUxWt6zAgAAAAAAAAAAAhAwLjAwMDAwMAAAAAAC1LMCAg4CCmZhbHNlApyY9Z6mZAKAgOb+gZEe",
    "AAAABhciAjYyMDIzLTAzLTI3VDExOjI3OjQ0LjQwMDg1NVoCNjIwMjMtMDMtMjdUMTE6Mjc6NDQuNDAwODY1WuavAgIoQWRlbGFpZGUgcm9hZCAmIE5hYXMCcjcgQWRlbGFpZGUgUm9hZCBEdWJsaW4gMixGcmlhcnkgaG91c2UgLCBOYWFzLCBDby4gS2lsZGFyZQIgRHVibGluICYgS2lsZGFyZQIgRHVibGluICYgS2lsZGFyZQACHEVxdWl0eSByZWxlYXNlAgZFVVICHDM0NzAwMDAuMDAwMDAwAhoyNTAwMDAuMDAwMDAwAhpJbnRlcmVzdCBPbmx5AhBRdWF0ZXJseQL4AUZpcnN0IEZpeGVkIENoYXJnZSBvdmVyIDcgQWRlbGFpZGUgcm9hZCBhbmQgRnJpYXJ5IGhvdXNlIGFuZCBpbnRlcmVzdCBndWFyYW50ZWUgYW5kIGNvc3Qgb3ZlcnJ1biBndWFyYW50ZWUgZnJvbSB0aGUgcHJvbW90ZXIC3q8CArq1AgIWAgpmYWxzZQKcmPWepmQCgIDm/oGRHg==",
    "AAAABhceAjYyMDIzLTAzLTAxVDE0OjA4OjE5LjY1NjMwMVoCNjIwMjMtMDMtMDFUMTQ6MDg6MTkuNjU2MzI1WrKvAgIYRWFzdG9uIEhvdXNlAmxFYXN0b24gSG91c2UsIExvd2VyIEJyYW5jaCBSb2FkLCBUcmFtb3JlLCBDbyBXYXRlcmZvcmQCDlRyYW1vcmUCEldhdGVyZm9yZAACJkFjcXVpc2l0aW9uIEZpbmFuY2UCBkVVUgIaMzMwMDAwLjAwMDAwMAIaMzMwMDAwLjAwMDAwMAIaSW50ZXJlc3QgT25seQIOTW9udGhseQJ6Rmlyc3QgRml4ZWQgQ2hhcmdlIG92ZXIgRWFzdG9uIEhvdXNlIGFuZCBQRyBmcm9tIHRoZSBwcm9tb3RlcgLwrgICyrQCAhICCmZhbHNlApyY9Z6mZAKAgOb+gZEe,"
]