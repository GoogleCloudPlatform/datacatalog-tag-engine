create or replace table reference.SensitiveCategory(infotype string, category string, rank integer);
insert into reference.SensitiveCategory(infotype, category, rank) values('PERSON_NAME', 'PERSONAL_IDENTIFIABLE_INFORMATION', 0);
insert into reference.SensitiveCategory(infotype, category, rank) values('STREET_ADDRESS', 'PERSONAL_IDENTIFIABLE_INFORMATION', 0);
insert into reference.SensitiveCategory(infotype, category, rank) values('US_SOCIAL_SECURITY_NUMBER', 'PERSONAL_IDENTIFIABLE_INFORMATION', 0);
	
insert into reference.SensitiveCategory(infotype, category, rank) values('FDA_CODE', 'CLIENT_IDENTIFIABLE_INFORMATION', 1);
insert into reference.SensitiveCategory(infotype, category, rank) values('US_HEALTHCARE_NPI', 'CLIENT_IDENTIFIABLE_INFORMATION', 1);

insert into reference.SensitiveCategory(infotype, category, rank) values('PHONE_NUMBER', 'SENSITIVE_PERSONAL_DATA', 2);
insert into reference.SensitiveCategory(infotype, category, rank) values('DATE_OF_BIRTH', 'SENSITIVE_PERSONAL_DATA', 2);
insert into reference.SensitiveCategory(infotype, category, rank) values('EMAIL_ADDRESS', 'SENSITIVE_PERSONAL_DATA', 2);
insert into reference.SensitiveCategory(infotype, category, rank) values('AGE', 'SENSITIVE_PERSONAL_DATA', 2);		