\set ON_ERROR_STOP on

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '1';
INSERT INTO data (id, value) VALUES (1, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '1';
SELECT * FROM data WHERE id = 1;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '2';
INSERT INTO data (id, value) VALUES (2, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '2';
SELECT * FROM data WHERE id = 2;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '3';
INSERT INTO data (id, value) VALUES (3, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '3';
SELECT * FROM data WHERE id = 3;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '4';
INSERT INTO data (id, value) VALUES (4, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '4';
SELECT * FROM data WHERE id = 4;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '5';
INSERT INTO data (id, value) VALUES (5, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '5';
SELECT * FROM data WHERE id = 5;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '6';
INSERT INTO data (id, value) VALUES (6, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '6';
SELECT * FROM data WHERE id = 6;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '7';
INSERT INTO data (id, value) VALUES (7, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '7';
SELECT * FROM data WHERE id = 7;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '8';
INSERT INTO data (id, value) VALUES (8, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '8';
SELECT * FROM data WHERE id = 8;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '9';
INSERT INTO data (id, value) VALUES (9, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '9';
SELECT * FROM data WHERE id = 9;

---

\set ON_ERROR_STOP on

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '10';
INSERT INTO data (id, value) VALUES (10, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '10';
SELECT * FROM data WHERE id = 10;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '11';
INSERT INTO data (id, value) VALUES (11, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '11';
SELECT * FROM data WHERE id = 11;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '12';
INSERT INTO data (id, value) VALUES (12, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '12';
SELECT * FROM data WHERE id = 12;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '13';
INSERT INTO data (id, value) VALUES (13, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '13';
SELECT * FROM data WHERE id = 13;

---

SET SERVER ROLE TO 'primary';
SET SHARDING KEY TO '14';
INSERT INTO data (id, value) VALUES (14, 'value_1');

SET SERVER ROLE TO 'replica';
SET SHARDING KEY TO '14';
SELECT * FROM data WHERE id = 14;

---

SET SERVER ROLE TO 'primary';
SELECT 1;

SET SERVER ROLE TO 'replica';
SELECT 1;

set server role to 'replica';
SeT SeRver Role TO 'PrImARY';
select 1;

SET PRIMARY READS TO 'on';
SELECT 1;

SET PRIMARY READS TO 'off';
SELECT 1;

SET PRIMARY READS TO 'default';
SELECT 1;
