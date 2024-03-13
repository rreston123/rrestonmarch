CREATE OR REPLACE PACKAGE ESB.auto_drop_to_inactive_pkg as

  procedure automate_drop;

  procedure log_error(p_errmsg in varchar2);

end auto_drop_to_inactive_pkg;
/
