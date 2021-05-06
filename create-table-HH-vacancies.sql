create table webscraping.HH_vacancies
(id mediumint unsigned not null auto_increment,
dt datetime not null,
keyword varchar(100) not null,
vacancy_title varchar(120) not null,
vacancy_tags varchar(500)  null,
vacancy_experience varchar(100) null default '�� �������',
employment_mode varchar(120) null default '�� �������',
vacancy_salary varchar(100) null default '�� �������',
vacancy_description text null,
company_name varchar(120) not null,
company_link varchar(120) not null,
company_address varchar(300) null,
publish_place_and_time varchar(300) not null,

primary key (id),
index keyword_IDX (keyword)
);