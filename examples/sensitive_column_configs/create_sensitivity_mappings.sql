create or replace table reference.SensitiveCategory(infotype string, category string, rank integer);

insert into reference.SensitiveCategory(infotype, category, rank) values('FDA_CODE', 'Client_Identifiable_Information', 1);
insert into reference.SensitiveCategory(infotype, category, rank) values('US_HEALTHCARE_NPI', 'Client_Identifiable_Information', 1);

insert into reference.SensitiveCategory(infotype, category, rank) values('PERSON_NAME', 'Personal_Identifiable_Information', 2);
insert into reference.SensitiveCategory(infotype, category, rank) values('US_SOCIAL_SECURITY_NUMBER', 'Personal_Identifiable_Information', 2);
insert into reference.SensitiveCategory(infotype, category, rank) values('STREET_ADDRESS', 'Personal_Identifiable_Information', 2);

insert into reference.SensitiveCategory(infotype, category, rank) values('PHONE_NUMBER', 'Sensitive_Personal_Data', 3);
insert into reference.SensitiveCategory(infotype, category, rank) values('DATE_OF_BIRTH', 'Sensitive_Personal_Data', 3);
insert into reference.SensitiveCategory(infotype, category, rank) values('EMAIL_ADDRESS', 'Sensitive_Personal_Data', 3);
insert into reference.SensitiveCategory(infotype, category, rank) values('AGE', 'Sensitive_Personal_Data', 3);		