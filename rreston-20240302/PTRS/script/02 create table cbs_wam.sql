 --table
    
    CREATE TABLE CBS_WAM.PTRS_USERNAMES (
    TRAN_NO NUMBER,
    PTRS_USERNAME varchar2(20),
    CBS_USERNAME varchar2(20),
    PLANT varchar2(2),
    transaction_date timestamp,
    constraint PK_PTRSusernames primary key (TRAN_NO)
);

--create sequence

   CREATE SEQUENCE  ptrs_seq  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 50 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;

--trigger
create or replace TRIGGER ptrs_trig
              before insert on PTRS_USERNAMES
              for each row
              begin
                  if :new.tran_no is null then
                      select ptrs_seq.nextval into :new.tran_no from sys.dual;
                 end if;
              end;

--roles
CREATE ROLE CBS_OPERATING_UNIT;
GRANT CBS_OPERATING_UNIT to CBS_SMD_ADMIN with admin option;

insert into cbs_wam.cbs_page_roles_lib(code,page_number, granted_role)values(165,127,'CBS_OPERATING_UNIT');
insert into cbs_wam.cbs_page_roles_lib(code,page_number, granted_role)values(164,128,'CBS_OPERATING_UNIT');

commit;