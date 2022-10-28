create or replace table reference.infotype_selection(field_infotypes array<string>, notable_infotype STRING);

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['PERSON_NAME', 'FIRST_NAME', 'LAST_NAME', 'ETHNIC_GROUP'], 'PERSON_NAME');
	
insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['EMAIL_ADDRESS', 'PERSON_NAME', 'FIRST_NAME', 'LAST_NAME', 'ETHNIC_GROUP'], 'EMAIL_ADDRESS');

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['STREET_ADDRESS', 'PERSON_NAME', 'FIRST_NAME', 'LAST_NAME', 'ETHNIC_GROUP'], 'STREET_ADDRESS');

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['DATE_OF_BIRTH'], 'DATE_OF_BIRTH');

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['AGE'], 'AGE');

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['PHONE_NUMBER'], 'PHONE_NUMBER');

insert into reference.infotype_selection(field_infotypes, notable_infotype) 
	values(['CREDIT_CARD_NUMBER'], 'CREDIT_CARD_NUMBER');

	
create or replace table reference.infotype_classification(notable_infotypes array<string>, classification_result string, sensitivity_rank integer);

insert into reference.infotype_classification(notable_infotypes, classification_result, sensitivity_rank) 
	values(['PERSON_NAME', 'EMAIL_ADDRESS', 'STREET_ADDRESS', 'AGE', 'DATE_OF_BIRTH', 'CREDIT_CARD_NUMBER', 'SOCIAL_SECURITY_NUMBER', 
			'US_DRIVERS_LICENSE_NUMBER', 'UK_DRIVERS_LICENSE_NUMBER', 'JAPAN_DRIVERS_LICENSE_NUMBER', 
			'PASSWORD', 'LOCATION_COORDINATES', 'PHONE_NUMBER'], 
			'Sensitive_Personal_Identifiable_Information', 3);

insert into reference.infotype_classification(notable_infotypes, classification_result, sensitivity_rank) 
	values(['PERSON_NAME', 'EMAIL_ADDRESS', 'STREET_ADDRESS', 'AGE', 'DATE_OF_BIRTH', 'ETHNIC_GROUP', 'PHONE_NUMBER'], 
			'Personal_Identifiable_Information', 2);

insert into reference.infotype_classification(notable_infotypes, classification_result, sensitivity_rank) 
	values(['CREDIT_CARD_NUMBER', 'SOCIAL_SECURITY_NUMBER', 'US_DRIVERS_LICENSE_NUMBER', 'UK_DRIVERS_LICENSE_NUMBER', 
			'JAPAN_DRIVERS_LICENSE_NUMBER', 'PASSWORD', 'LOCATION_COORDINATES'], 'Sensitive_Personal_Information', 2);

insert into reference.infotype_classification(notable_infotypes, classification_result, sensitivity_rank) 
	values(['IP_ADDRESS', 'LOCATION', 'ETHNIC_GROUP'], 'Personal_Information', 1);	

insert into reference.infotype_classification(notable_infotypes, classification_result, sensitivity_rank) 
	values(['STREET_ADDRESS'], 'Public_Information', 0);		
