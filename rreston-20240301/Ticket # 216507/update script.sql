update cisadm.ci_adj_stg_up
   set adj_type_cd = 'CM-GSL1'
 where adj_stg_ctl_id = '76078'
   and adj_stg_up_status_flg = 'P'
    and adj_type_cd = 'CM-GSL0';
 
 commit;