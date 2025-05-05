"""
====================================================================================
File                :   queries.py
Description         :   Contains all queries
Author              :   Safvan C K
Date Created        :   Feb 27th 2023
Last Modified BY    :   Safvan C K
Date Modified       :   Mar 22nd 2023
====================================================================================
"""

# ======================================================================================================================
get_test_data = 'select * from public.demo t;'
insert_test_data = "INSERT INTO public.demo (name) VALUES('{col1}');"
insert_employee_date = "INSERT INTO PUBLIC.employee_master (EMP_BU) VALUES ('{EMP_BU}');"
# ======================================================================================================================

# Users Queries
# ======================================================================================================================
insert_user_sql = """delete from public.usr_cls uc where uc.cre_on::date <= now()- interval '1 DAY' and 
UC.sign_up = false; 
insert into public.usr_cls (nick_name, gender, mob_no, email, usr_img_path, 
usr_type, birthday, otp_m, otp_e, usr_token) VALUES ('{nn}', '{gn}', '{mn}', '{em}', '{up}', '{ut}', '{bd}', 
{om},{oe}, '{uk}');"""

get_users_sql = """select user_id, nick_name, gender, mob_no, email, usr_img_path, usr_type, birthday from 
public.usr_cls uc where uc.sign_up = false; """

validate_otp_sql = """update public.usr_cls set otp_m = null, otp_e = null, sign_up = false
where mob_no = '{mn}' and otp_m = {om} and email = '{em}' and otp_e = {oe};"""
# ======================================================================================================================

# Sureksha Queries
# ======================================================================================================================
get_user_data = """select * from user_master where user_name = '{user_name}' and show_password = '{show_password}';"""
user_validation = """ SELECT * from user_master where user_access_token =  '{access_token}' """
update_user_data = """update user_master set user_access_token = '{tkn}' where user_id = '{id}';"""
update_user_password_change = """update user_master set user_access_token = '{tkn}',show_password = '{shpass}',password = '{pas}' where user_id = '{id}';"""

a_s_users = """ SELECT count(*) as count
FROM user_master
WHERE user_master.id != '' {search_join};"""

all_s_users = """ SELECT user_id,user_name,first_name,last_name,show_password,DATE_FORMAT(user_master.created_date, '%b %d, %Y | %l:%i %p') as created_f_date,user_master.created_date,user_master.user_type,mail_id,phone_number,address,user_master.active_status
FROM user_master
WHERE user_master.id != ''  {search_join} {order_by} {limit};"""

update_user_data2 = """ update user_master set first_name = '{fn}', last_name = '{ln}', user_name = '{un}',password = '{pw}', show_password = '{spw}', phone_number = '{phn}',mail_id = '{mlid}',address = '{adrs}',active_status={ats} where user_id = '{uid}'"""

insert_user_data = """ insert into user_master (user_id, first_name, last_name, user_name, password, show_password, phone_number, mail_id, address,user_type,active_status,created_date) values ('{uid}','{fn}','{ln}','{un}','{pw}','{spw}','{phn}','{mlid}','{adrs}','{usrt}',{ats},'{cdt}');"""
insert_user_data_privilages = """ update user_master set privilages = '{fn}', useraccesslist = '{ln}' where user_id = '{uid}'"""
get_user_name = """ select * from user_master where user_name = '{un}';"""
get_user_name_id = """ select * from user_master where user_name = '{un}' and user_id != '{uid}';"""
get_mobile = """ select * from user_master where phone_number = '{mb}';"""
get_mobile_id = """ select * from user_master where phone_number = '{mb}' and user_id != '{uid}';"""
get_mail = """ select * from user_master where mail_id = '{ml}';"""
get_mail_id = """ select * from user_master where mail_id = '{ml}' and user_id != '{uid}';"""
update_user_data2 = """ update user_master set first_name = '{fn}', last_name = '{ln}', user_name = '{un}',password = '{pw}', show_password = '{spw}', phone_number = '{phn}',mail_id = '{mlid}',address = '{adrs}',active_status={ats} where user_id = '{uid}'"""
update_user_password_change = """update user_master set user_access_token = '{tkn}',show_password = '{shpass}',password = '{pas}' where user_id = '{id}';"""


insert_product_master = """ insert into product_master (product_id,rate,product_name,active_status,created_by,created_date) values ('{id}','{rt}','{pn}',{ats},'{un}','{cdt}');"""
update_product_master = """ update product_master set created_by = '{un}',product_name = '{pn}', active_status = '{ats}',rate = '{rt}' where product_id = '{id}';"""

single_product_dtls = """ select *,DATE_FORMAT(product_master.created_date, '%b %d, %Y | %l:%i %p') as created_f_date from product_master where product_id = '{id}'; """

get_s_product_dtls = """SELECT *,product_master.active_status as active_status,DATE_FORMAT(product_master.created_date, '%b %d, %Y | %l:%i %p') as created_f_date, product_name as name 
FROM product_master
WHERE product_master.id != ''  {search_join} {order_by} {limit};"""

get_s_product = """SELECT count(*) as count
FROM product_master
WHERE product_master.id != '' {search_join};"""

order_count = """SELECT COUNT(*) as order_count
FROM order_table
WHERE DATE(created_date) = '{dt}';"""

insert_bill_data = """ insert into order_table (order_number,order_id,total_amount,gst,product_data,extra,mode_of_payment,bill_type,customer_id,created_date) values ('{on}','{oid}',{tm},{gst},"{pd}",'{ex}','{pmd}','{dtype}','{cno}','{cdt}');"""

update_bill = """ update order_table set cancel_status = {cs}, cancel_note = '{cn}' where order_id = '{oid}';"""
insert_bill_product_data = """ insert into bill_items_table (ref_order_id,product_name,ref_product_id,quantity,rate,total_amount,original_rate,original_total,created_date) values ('{oid}','{pn}','{rpid}',{qty},{rt},{tm},{orgrt},{orgtt},'{cdt}');"""

update_items_table = """ update bill_items_table set cancel_status = {cs} where ref_order_id = '{oid}';"""

a_s_bill = """ SELECT count(*) as count
FROM order_table
WHERE order_table.id != '' {search_join};"""

all_s_bill = """ SELECT *,DATE_FORMAT(order_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date,order_table.created_date
FROM order_table
WHERE order_table.id != ''  {search_join} {order_by} {limit};"""


a_s_items = """ SELECT COUNT(*) as count FROM ( SELECT product_name, SUM(quantity) AS quantity, SUM(original_total) AS total FROM bill_items_table WHERE bill_items_table.id != '' {search_join} GROUP BY product_name ) AS subquery;"""

# all_s_items = """ SELECT *,DATE_FORMAT(bill_items_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date,bill_items_table.created_date
# FROM bill_items_table
# WHERE bill_items_table.id != ''  {search_join} {order_by} {limit};"""

all_s_items = """ SELECT product_name, COUNT(DISTINCT ref_order_id) AS order_count,DATE_FORMAT(bill_items_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date, SUM(quantity) AS quantity, SUM(original_total) AS total FROM bill_items_table WHERE bill_items_table.id != '' {search_join} GROUP BY product_name {order_by} {limit};"""

single_bill = """ select *,DATE_FORMAT(order_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date from order_table where order_id = '{uid}';"""
search_product_by_id = """ select * from product_master where product_id = '{id}';"""
single_bill_product = """ select *,DATE_FORMAT(bill_items_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date from bill_items_table where ref_order_id = '{uid}';"""

insert_into_log = """ insert into log_data (log_id,action_type,action_on,action_by,ref_action_on_id) values ('{lid}','{act}','{aco}','{acb}','{racid}');"""
# department_master --- id, dept_id, department_name, active_status, created_date, privilages
last_bill_qry = """SELECT *,DATE_FORMAT(order_table.created_date, '%b %d, %Y | %l:%i %p') as created_f_date FROM order_table ORDER BY id DESC LIMIT 1; """
# designation_master ---- id, ref_dept_id, desig_id, desig_name, active_status, created_date, privilages
