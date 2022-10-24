
Understanding the code:

It is recommended you read types.py for a good example:

This parses and formats the SQL types, including a STRUCT<>, VARCHAR(), and ARRAY<> type.

This will give you a good idea of how to think about parsing.

Note on terminology:
 - parse() means that something is *expected* and an error should be thrown if it's
   not there. The parsed result will always be returned or an error thrown.
 - consume() means that something is optional, and the value will either be returned
   or None if it's not available.
 - lex.error('message') will throw and report an error at the current parsing location
   with the error message. The first error causes all parsing to stop.

