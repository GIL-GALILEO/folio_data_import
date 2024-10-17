import pymarc

def prepend_ppn_prefix_001(record: pymarc.Record) -> pymarc.Record:
    """
    Prepend the PPN prefix to the record's 001 field. Useful when
    importing records from the ABES SUDOC catalog

    Args:
        record (pymarc.Record): The MARC record to preprocess.

    Returns:
        pymarc.Record: The preprocessed MARC record.
    """
    record['001'].data = '(PPN)' + record['001'].data
    return record

def strip_999_ff_fields(record: pymarc.Record) -> pymarc.Record:
    """
    Strip all 999 fields with ff indicators from the record.
    Useful when importing records exported from another FOLIO system

    Args:
        record (pymarc.Record): The MARC record to preprocess.

    Returns:
        pymarc.Record: The preprocessed MARC record.
    """
    for field in record.get_fields('999'):
        if field.indicators == pymarc.Indicators(*['f', 'f']):
            record.remove_field(field)
    return record
