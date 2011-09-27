create table sale (
saleID integer primary key,
SKU varchar(200) not null,
developer varchar(200) not null,
title varchar(200) not null,
units integer not null,
developerProceeds decimal(2,2),
saleDate text not null,
currencyOfProceeds varchar(10),
customerPrice decimal(2,2)
);
