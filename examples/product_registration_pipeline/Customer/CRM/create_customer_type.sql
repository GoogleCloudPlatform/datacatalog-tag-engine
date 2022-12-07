create or replace table reference.CustomerType(
	tier_index STRING,
	tier_name STRING
);

insert into reference.CustomerType(tier_index, tier_name) values('0', 'Bronze');
insert into reference.CustomerType(tier_index, tier_name) values('1', 'Platinum');
insert into reference.CustomerType(tier_index, tier_name) values('2', 'Gold');
insert into reference.CustomerType(tier_index, tier_name) values('3', 'Select');