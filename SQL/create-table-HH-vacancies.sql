create database webscraping;
use webscraping;

create table webscraping.HH_vacancies
(id mediumint unsigned not null auto_increment,
dt datetime not null,
keyword varchar(100) not null,
vacancy_title varchar(120) not null,
vacancy_tags varchar(1000)  null,
vacancy_experience varchar(100) null default 'íå óêàçàíî',
employment_mode varchar(120) null default 'íå óêàçàíî',
vacancy_salary varchar(100) null default 'íå óêàçàíî',
vacancy_description text null,
company_name varchar(120) not null,
company_link varchar(120) not null,
company_address varchar(300) null,
publish_place_and_time varchar(300) not null,

primary key (id),
index keyword_IDX (keyword)
);

alter table webscraping.HH_vacancies add column uid varchar(100) null unique;
