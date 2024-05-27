-- Should be the inverse of formatReadableSize
SELECT formatReadableSize(fromReadableSize('1 B'));
SELECT formatReadableSize(fromReadableSize('1 KiB'));
SELECT formatReadableSize(fromReadableSize('1 MiB'));
SELECT formatReadableSize(fromReadableSize('1 GiB'));
SELECT formatReadableSize(fromReadableSize('1 TiB'));
SELECT formatReadableSize(fromReadableSize('1 PiB'));
SELECT formatReadableSize(fromReadableSize('1 EiB'));

-- Is case-insensitive
SELECT formatReadableSize(fromReadableSize('1 mIb'));

-- Should be able to parse decimals
SELECT fromReadableSize('1.00 KiB');    -- 1024
SELECT fromReadableSize('3.00 KiB');    -- 3072

-- Should be able to parse negative numbers
SELECT fromReadableSize('-1.00 KiB');    -- 1024

-- Infix whitespace is ignored
SELECT fromReadableSize('1    KiB');
SELECT fromReadableSize('1KiB');

-- Can parse LowCardinality
SELECT fromReadableSize(toLowCardinality('1 KiB'));

-- Can parse nullable fields
SELECT fromReadableSize(toNullable('1 KiB'));

-- Can parse non-const columns fields
SELECT fromReadableSize(materialize('1 KiB'));

-- Output is NULL if NULL arg is passed
SELECT fromReadableSize(NULL);

-- ERRORS
-- No arguments
SELECT fromReadableSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Too many arguments
SELECT fromReadableSize('1 B', '2 B'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Wrong Type
SELECT fromReadableSize(12); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Invalid input - overall garbage
SELECT fromReadableSize('oh no'); -- { serverError CANNOT_PARSE_NUMBER }
-- Invalid input - unknown unit
SELECT fromReadableSize('12.3 rb'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Leading whitespace
SELECT fromReadableSize(' 1 B'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
-- Invalid input - Trailing characters
SELECT fromReadableSize('1 B leftovers'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
