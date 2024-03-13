CREATE OR REPLACE PACKAGE BODY ESB.auto_drop_to_inactive_pkg is
  /*
    Author   : Kenneth V. Diones
    Purpose  : CM#1226 Automate Drop to Inactive (STOP SA)
               This package is use for eliminating issues on delays on updates,
               dropping to Inactive of accounts will be done on a set period of time rather than carrying it out upon meter removal.
               This will only apply to all residential customers.
    Overview : DB Job scheduler will call this package.

    REVISION HISTORY

    v1.0.4 26-March-2020 KDiones
      Purpose of Change : additional criteria on the sql of the cursor statement add fa status flg completed.
      Affected Objects : automate_drop
    v1.0.3 06-March-2020 KDiones
      Purpose of Change : add function 'add_months' to add 30 days of the v_due_dt variable.
      Affected Objects : automate_drop
    v1.0.2 28-February-2020 KDiones
      Purpose of Change : Removes unnecessary convertion of date.
      Affected Objects  : automate_drop
    v1.0.1 27-February-2020 KDiones
      Purpose of Change : change if and else condition from greater than equal'=>' to less than equal '<=' .
      Affected Objects  : automate_drop
    v1.0.0 28-January-2020 KDiones
      Purpose of Change : Create procedures
      Affected Objects  : new:automate_drop
                          new:log_error
  */

  procedure log_error(p_errmsg in varchar2) is
    /*
      REVISION HISTORY
      v1.0.0 28-January-2020 KDiones
        Remarks : This will logged all errors encoutered.
    */
    pragma autonomous_transaction;
  begin
    insert into error_logs
      (logged_by, logged_on, module, custom_error_msg)
    values
      (user, sysdate, 'AUTO_DROP_TO_INACTIVE_PKG.AUTOMATE_DROP', p_errmsg);
    commit;
  end log_error;

  procedure automate_drop is
    /*
      REVISION HISTORY
       v1.0.4 26-March-2020 KDiones
      Remarks : additional criteria on the sql of the cursor statement add fa_status_flg.
      v1.0.0 28-January-2020 KDiones
        Remarks : This will automate the dropping of account to inactive.
    */
    -->> 1. select candidate accounts for dropping to inactive
    cursor cur_temp is
      select fa.fa_id,
             fa.sp_id,
             trim(fa.cre_dttm) cre_dttm,
             sa.sa_id,
             cfcd.adhoc_char_val due_dt,
             (select a.adhoc_char_val
                from ci_fa_char a
               where a.fa_id = fa.fa_id
                 and a.char_type_cd = 'DISCONMR') meter_read
        from ci_sa      sa,
             ci_sp      sp,
             ci_fa      fa,
             ci_sa_sp   sasp,
             ci_sa_type st,
             ci_fa_char cfcd
       where not exists (select null
                from (select sp_id,
                             usage_flg,
                             uom_cd,
                             end_read_dttm,
                             end_reg_reading,
                             DENSE_RANK() OVER(PARTITION BY sp_id ORDER BY end_read_dttm desc) dense_rank
                        from ci_bseg_read
                       where uom_cd = 'KW'
                         and end_reg_reading > 0
                         and usage_flg = 'X')
               where dense_rank = 1
                 and sp_id = sasp.sp_id)
         and fa.fa_id = cfcd.fa_id(+)
         and fa.sp_id = sasp.sp_id(+)
         and sasp.sa_id = sa.sa_id(+)
         and sasp.sp_id = sp.sp_id(+)
         and sa.sa_type_cd = st.sa_type_cd
         and fa.fa_type_cd = 'SEV-CUT'
         and fa.sched_dttm =
             (select max(sched_dttm)
                from ci_fa cf
               where cf.fa_type_cd = fa.fa_type_cd
                 and cf.sp_id = fa.sp_id
                 and cf.fa_status_flg = fa.fa_status_flg)
         and fa.fa_status_flg = 'C'
         and st.svc_type_cd = 'EL'
         and st.dst_id = 'A/R-ELEC'
         and cfcd.char_type_cd = 'DISCONDT'
         and sa.sa_status_flg = '20'
         and sp.sp_src_status_flg = 'D '
         and sp.sp_status_flg = 'R '
         and sasp.usage_flg = '+'
         and cfcd.adhoc_char_val not in
             ('X',
              '11-11-1111 11:11',
              '01-01-1900 12:00',
              '01-01-1900 00:00');

    type cur_tab is table of cur_temp%rowtype;
    cur_lst      cur_tab;
    l_error_line number;

    l_raise_exception EXCEPTION;
    l_errmsg varchar2(2000);
  begin
    l_error_line := 10;
    open cur_temp;
    fetch cur_temp bulk collect
      into cur_lst;
    close cur_temp;

    for x in 1 .. cur_lst.count loop
      declare
        v_due_dt     date;
        v_due_days   number;
        v_meter_read number;
      begin

        l_error_line := 20;

        v_due_dt := trunc(to_date(substr(cur_lst(x).due_dt, 1, 10),
                                  'mm/dd/yyyy'));

        v_meter_read := to_number(cur_lst(x).meter_read, '999999.99');
        /*
            VERSION HISTORY
               v1.0.2 28-February-2020 KDiones
                Remarks : Removes unnecessary convertion of date.
        */
        l_error_line := 30;
        select trunc(sysdate) - v_due_dt into v_due_days from dual;
        /*
            VERSION HISTORY
               v1.0.3 06-March-2020 KDiones
                Remarks : add function 'add_months' to add 30 days of the v_due_dt variable.
               v1.0.1 27-February-2020 KDiones
                Remarks : change if and else condition from greater than equal'=>' to less than equal '<=' .
        */
        if (cur_lst(x).cre_dttm <= v_due_dt) then

          v_due_dt := add_months(v_due_dt, 1);

          if v_due_days > 30 then

            l_error_line := 40;
            -->> 2. Stop SA/SP Connection
            update ci_sa_sp
               set stop_dttm = v_due_dt
             where sa_sp_id = (select sasp.sa_sp_id
                                 from ci_sa_sp sasp
                                where sasp.sa_id = cur_lst(x).sa_id
                                  and sasp.stop_dttm is null);

            l_error_line := 50;
            -->> 3. Stop SA
            update ci_sa
               set sa_status_flg = '40', end_dt = v_due_dt
             where sa_id = (select sasp.sa_id
                              from ci_sa_sp sasp
                             where sasp.sa_id = cur_lst(x).sa_id
                               and sasp.stop_dttm is not null);

            l_error_line := 60;
            -->> 4. Stop MDM equivalent for SA
            update d1_us
               set bo_status_cd = 'INACTIVE', end_dttm = v_due_dt
             where us_id = (select us_id
                              from D1_US_SP
                             where d1_sp_id =
                                   (select d1_sp_id
                                      from d1_sp_identifier
                                     where id_value = cur_lst(x).sp_id
                                       and sp_id_type_flg = 'D1EI')
                               and d1_stop_dttm is null);

            l_error_line := 70;
            -->> 5. Stop MDM equivalent for SA/SP Connection
            update D1_US_SP
               set d1_stop_dttm = v_due_dt
             where d1_sp_id = (select d1_sp_id
                                 from d1_sp_identifier
                                where id_value = cur_lst(x).sp_id
                                  and sp_id_type_flg = 'D1EI')
               and d1_stop_dttm is null;

            -->> 6. Upload Meter Read

            declare
              l_badge_nbr      varchar2(30);
              l_mr_source_cd   varchar2(12) := 'MRD'; -->> for verification
              l_use_on_bill_sw varchar2(1) := 'Y';
              l_mtr_reader_id  varchar2(20) := substr('SYSTEM', 1, 20); -->> for verification
              l_read_type_flg  varchar2(2) := '60';
              l_review_hilo_sw varchar2(1) := 'N';

              l_kwh_rdg  number := v_meter_read;
              l_kw_rdg   number := 0;
              l_kvar_rdg number := 0;

              l_mr_stage_up_id number;
              l_kw_reg_seq     number;
              l_kwh_reg_seq    number;
              l_kvar_reg_seq   number;
            begin
              -->> get meter badge_nbr
              l_error_line := 80;
              select mtr.badge_nbr
                into l_badge_nbr
                from ci_mtr mtr, ci_mtr_config mc, ci_sp_mtr_hist spmh
               where mtr.mtr_id = mc.mtr_id
                 and mc.mtr_config_id = spmh.mtr_config_id
                 and spmh.removal_dttm is null
                 and spmh.sp_id = cur_lst(x).sp_id;

              -->>  get l_kw_reg_seq, l_kwh_reg_seq, l_kvar_reg_seq
              begin
                l_error_line := 90;
                select max(decode(reg.uom_cd, 'KW  ', reg.read_seq)),
                       max(decode(reg.uom_cd, 'KWH ', reg.read_seq)),
                       max(decode(reg.uom_cd, 'KVAR', reg.read_seq))
                  into l_kw_reg_seq, l_kwh_reg_seq, l_kvar_reg_seq
                  from ci_reg reg, ci_mtr mtr, ci_mtr_config mc
                 where reg.mtr_id = mtr.mtr_id
                   and mtr.mtr_id = mc.mtr_id
                   and mtr.badge_nbr = rpad(l_badge_nbr, 30, ' ');
              exception
                when no_data_found then
                  l_kw_reg_seq   := null;
                  l_kwh_reg_seq  := null;
                  l_kvar_reg_seq := null;
              end;

              --> insertion in ci_mr_stage_up
              if l_kw_reg_seq is not null or l_kwh_reg_seq is not null or
                 l_kvar_reg_seq is not null then
                loop
                  begin
                    l_error_line := 100;
                    select ci_mrstgupid_seq.nextval
                      into l_mr_stage_up_id
                      from dual;

                    l_error_line := 110;
                    insert into ci_mr_stage_up
                      (mr_stage_up_id,
                       badge_nbr,
                       read_dttm,
                       mr_up_status_flg,
                       mr_source_cd,
                       use_on_bill_sw,
                       mtr_reader_id)
                    values
                      (l_mr_stage_up_id,
                       l_badge_nbr,
                       v_due_dt,
                       'P',
                       l_mr_source_cd,
                       l_use_on_bill_sw,
                       l_mtr_reader_id);
                    exit;

                  exception
                    when dup_val_on_index then
                      null;
                    when others then
                      raise l_raise_exception;
                  end;
                end loop;
              end if;

              -->> insertion in l_kwh_reg_seq
              if l_kwh_reg_seq is not null then
                begin
                  l_error_line := 120;
                  insert into ci_rr_stage_up
                    (mr_stage_up_id,
                     read_seq,
                     read_type_flg,
                     uom_cd,
                     reg_reading,
                     review_hilo_sw)
                  values
                    (l_mr_stage_up_id,
                     l_kwh_reg_seq,
                     l_read_type_flg,
                     'KWH',
                     l_kwh_rdg,
                     l_review_hilo_sw);

                exception
                  when dup_val_on_index then
                    null;
                  when others then
                    raise l_raise_exception;
                end;
              end if;

              -->> insertion in l_kvar_reg_seq
              if l_kvar_reg_seq is not null then
                begin
                  l_error_line := 130;
                  insert into ci_rr_stage_up
                    (mr_stage_up_id,
                     read_seq,
                     read_type_flg,
                     uom_cd,
                     reg_reading,
                     review_hilo_sw)
                  values
                    (l_mr_stage_up_id,
                     l_kvar_reg_seq,
                     l_read_type_flg,
                     'KVAR',
                     l_kvar_rdg,
                     l_review_hilo_sw);

                exception
                  when dup_val_on_index then
                    null;
                  when others then
                    raise l_raise_exception;
                end;
              end if;

              -->> insertion in l_kw_reg_seq
              if l_kw_reg_seq is not null then
                begin
                  l_error_line := 140;
                  insert into ci_rr_stage_up
                    (mr_stage_up_id,
                     read_seq,
                     read_type_flg,
                     uom_cd,
                     reg_reading,
                     review_hilo_sw)
                  values
                    (l_mr_stage_up_id,
                     l_kw_reg_seq,
                     l_read_type_flg,
                     'KW',
                     l_kw_rdg,
                     l_review_hilo_sw);
                exception
                  when dup_val_on_index then
                    null;
                  when others then
                    raise l_raise_exception;
                end;
              end if;

              l_error_line := 150;
              -->> insertion in ci_mrr_stge_up
              insert into ci_mrr_stge_up
                (mr_stage_up_id, reader_rem_cd)
              values
                (l_mr_stage_up_id, 'F00');

              l_error_line := 369;
              insert into ci_mrr_stge_up
                (mr_stage_up_id, reader_rem_cd)
              values
                (l_mr_stage_up_id, 'M0');

              l_error_line := 160;
              -->> insertion in ci_mr_stgup_char
              insert into ci_mr_stgup_char
                (mr_stage_up_id,
                 char_type_cd,
                 seq_num,
                 adhoc_char_val,
                 srch_char_val)
              values
                (l_mr_stage_up_id, 'RDTRYCNT', 1, 1, 1);

              l_error_line := 170;
              insert into ci_mr_stgup_char
                (mr_stage_up_id,
                 char_type_cd,
                 seq_num,
                 adhoc_char_val,
                 srch_char_val)
              values
                (l_mr_stage_up_id,
                 'MR_REM',
                 1,
                 'REMOVAL READING',
                 'REMOVAL READING');
            end;
          end if;
        end if;

      exception
        when dup_val_on_index then
          null;
        when others then
          rollback;
          l_errmsg := 'Error in esb.auto_drop_to_inactive_pkg.automate_drop @ line: ' ||
                      l_error_line || ' - ' || sqlerrm;
          log_error(l_errmsg);
          raise_application_error(-20000, l_errmsg);
      end;

    end loop;
    commit;

  exception
    when others then
      null;
  end automate_drop;
end auto_drop_to_inactive_pkg;
/
