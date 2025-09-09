-- MySQL schema for telco customer churn dataset
create database if not exists telco_churn;
use telco_churn;

-- drop tables if exists (for development)
drop table if exists customer_profile;
drop table if exists demographics;
drop table if exists `location`;
drop table if exists `population`;
drop table if exists services;
drop table if exists financials;

-- main customer churn table
create table customer_profile (
    customer_id varchar(20) not null primary key unique,
    tenure_months tinyint unsigned not null default 0
);

-- demographics table
create table demographics (
    customer_id varchar(20) not null primary key unique,

    -- demographic info
    gender enum('Male', 'Female', 'Other') not null,
    age int unsigned not null,
    is_under_30 boolean not null,
    is_senior_citizen boolean not null,
    has_partner boolean not null,
    has_dependents boolean not null,
    number_of_dependents int unsigned not null default 0,

    foreign key (customer_id) references customer_profile(customer_id)
);

-- location table
create table location (
    customer_id varchar(20) not null primary key unique,

    -- location info
    country varchar(100) not null,
    `state` varchar(100) not null,
    city varchar(100) not null,
    zip_code char(10) not null,

    foreign key (customer_id) references customer_profile(customer_id)
);

-- population table
create table population (
    zip_code char(10) not null primary key unique,

    -- population info
    population int unsigned not null,

    foreign key (zip_code) references location(zip_code)
);

-- services table
create table services (
    customer_id varchar(20) not null primary key unique,
    has_referred_a_friend boolean not null,
    number_of_referrals int unsigned not null default 0,

    -- phone service
    has_phone_service boolean not null,
    avg_monthly_long_distance_charges decimal(6,2) not null check (avg_monthly_long_distance_charges >= 0),
    has_multiple_lines boolean not null,

    -- internet service
    has_internet_service boolean not null,
    internet_service_type enum('None', 'DSL', 'Fiber optic') not null,
    avg_monthly_gb_download decimal(10,2) not null check (avg_monthly_gb_download >= 0),

    -- internet addons
    has_online_security boolean not null,
    has_online_backup boolean not null,
    has_device_protection boolean not null,
    has_tech_support boolean not null,
    has_tv boolean not null,
    has_movies boolean not null,
    has_music boolean not null,
    has_unlimited_data boolean not null,

    foreign key (customer_id) references customer_profile(customer_id)
);

-- financials table (from the original services table)
create table financials (
    customer_id varchar(20) not null primary key unique,
    
    -- account info
    contract_type enum('Month-to-month', 'One year', 'Two year') not null,
    has_paperless_billing boolean not null,
    payment_method enum(
        'Bank Withdrawal',
        'Credit Card',
        'Mailed Check'
    ) not null,
    
    -- financials
    monthly_charges decimal(6,2) not null check (monthly_charges >= 0),
    total_charges decimal(10,2) not null check (total_charges >= 0),
    total_refunds decimal(10,2) not null check (total_refunds >= 0),
    total_extra_data_charges int not null check (total_extra_data_charges >= 0),
    total_long_distance_charges decimal(10,2) not null check (total_long_distance_charges >= 0),
    total_revenue decimal(10,2) not null check (total_revenue >= 0),
    foreign key (customer_id) references customer_profile(customer_id)
);

create table status (
    customer_id varchar(20) not null primary key unique,

    -- status
    satisfaction_score tinyint unsigned not null check (satisfaction_score between 0 and 6),
    churn_label boolean not null,
    churn_score int not null check (churn_score >= 0 and churn_score <= 100),
    cltv int not null check (cltv >= 0),
    churn_category enum(
        '',
        'Competitor',
        'Dissatisfaction',
        'Attitude',
        'Price',
        'Other'
    ) not null,
    
    foreign key (customer_id) references customer_profile(customer_id)
);