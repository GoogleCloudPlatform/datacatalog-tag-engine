create table enterprise_glossary.mapping(
	source_name STRING,
	canonical_name STRING
); 

/* Farm Data mappings */
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Cust_Key', 'source_supplier_unique_id');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Company_name', 'primary_business_name');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Primary_Address_1', 'organization_address_street_address_line1');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Primary_City', 'organization_address_city');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Primary_State', 'organization_address_state');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Primary_Zip_code', 'organization_address_postal_code');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Primary_Country_name', 'organization_address_country');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Phone_number', 'telephone_number');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Contact_First_Name', 'chief_executive_officer_first_name');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Contact_Last_Name', 'chief_executive_officer_last_name');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Contact_Suffix', 'chief_executive_officer_name_suffix');		
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Industry_Code', 'sic_code_1');	

/* USA Data mappings. Note: removed redundant and incomplete mappings (e.g. Fax) */
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Index', 'source_supplier_unique_id');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Address', 'organization_address_street_address_line1');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('City', 'organization_address_city');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('State', 'organization_address_state');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('ZipCode', 'organization_address_postal_code');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Phone', 'telephone_number');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('ContactFirstName', 'chief_executive_officer_first_name');
insert into enterprise_glossary.mapping (source_name, canonical_name) values('ContactLastName', 'chief_executive_officer_last_name');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('FullTitle', 'chief_executive_officer_title');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('SIC6', 'sic_code_1_base_6');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('CountyName', 'organization_address_county');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('MSAName', 'organization_msa_name');		
insert into enterprise_glossary.mapping (source_name, canonical_name) values('SIC2', 'sic_code_1_base_2');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('SIC4', 'sic_code_1_base_4');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('SICDescription', 'sic_code_1_base_4_description');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('BusinessStartYear', 'year_started');		
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Title', 'chief_executive_officer_title');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('Zip4', 'organization_address_postal_code_4_extension');	
insert into enterprise_glossary.mapping (source_name, canonical_name) values('WebsiteURL', 'company_url');	
		