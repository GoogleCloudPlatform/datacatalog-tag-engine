create or replace table enterprise_glossary.glossary_attributes(column_name string, formal_name string, definition string, synonym string, acronym string, url string);

insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('Address', 'organization_address_street_address_line1');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('City', 'organization_address_city');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('State', 'organization_address_state');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('ZipCode', 'organization_address_postal_code');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('Phone', 'telephone_number');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('ContactFirstName', 'chief_executive_officer_first_name');
insert into enterprise_glossary.glossary_attributes(column_name, formal_name) values('ContactLastName', 'chief_executive_officer_first_name');