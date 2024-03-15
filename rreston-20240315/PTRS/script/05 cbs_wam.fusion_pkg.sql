create or replace PACKAGE FUSION_PKG AS 
 
  PROCEDURE is_coa_valid(p_gl_combination IN VARCHAR2,
                         p_status         OUT VARCHAR2);
 
END;

/

create or replace PACKAGE BODY fusion_pkg AS

    PROCEDURE is_coa_valid(p_gl_combination IN VARCHAR2,
                         p_status         OUT VARCHAR2) AS
    l_line                NUMBER;
    l_api_password        VARCHAR2(100);
    l_api_username        VARCHAR2(100);
    l_api_wallet_path     VARCHAR2(100);
    l_api_wallet_password VARCHAR2(100);
    l_custom_url            VARCHAR2(5000);
    l_response_body       CLOB;
    l_response_var        varchar2(5000);
    l_enabled_flag        varchar2(100);
    l_error_code          varchar2(100);
  BEGIN
    l_line               := 10;
    l_custom_url := registries_pkg.get_active_registry('DEFAULT_FUSION_CUSTOM_URL');
    l_api_username        := registries_pkg.get_active_registry('DEFAULT_OIC_USERNAME');
    l_api_password        :=  registries_pkg.get_active_registry('DEFAULT_OIC_PASSWORD');
    l_api_wallet_path     := registries_pkg.get_active_registry('DEFAULT_API_WALLET_PATH');
    l_api_wallet_password := registries_pkg.get_active_registry('DEFAULT_API_WALLET_PASS');
  
    l_line := 20;
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
  
    l_response_body := apex_web_service.make_rest_request(p_url => l_custom_url,
                                                          p_http_method => 'GET',
                                                          p_username => l_api_username,
                                                          p_password => l_api_password,
                                                          p_parm_name => APEX_UTIL.string_to_table('p_gl_combination'),
                                                          p_parm_value => APEX_UTIL.string_to_table(p_gl_combination),
                                                          p_wallet_path => l_api_wallet_path,
                                                          p_wallet_pwd => l_api_wallet_password);
  
    SELECT dbms_lob.substr(l_response_body, 4000, 1)
      into l_response_var
      FROM dual;
  
    --dbms_output.put_line(l_response_var);
  
    SELECT JSON_VALUE(l_response_body, '$.details.enabled_flag')
      INTO l_enabled_flag
      FROM dual;
    --dbms_output.put_line('enabled_flag: ' || l_enabled_flag);
  
    IF l_enabled_flag = 'Y' THEN
      p_status := 'VALID';
    elsif l_enabled_flag = 'N' THEN
      p_status := 'INVALID';
    else
      SELECT JSON_VALUE(l_response_body, '$."o:errorCode"')
        INTO l_error_code
        FROM dual;
      --dbms_output.put_line('errorCode: ' || l_error_code);
    
      if trim(l_error_code) is not null then
        p_status := 'ERROR';
      else
        p_status := 'INVALID';
      end if;
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_status := 'ERROR';
      audit_logs_pkg.error_logs(application_id_in => 2117,-- >> cbs applicaton
                                sqlcode_in => sqlcode,
                                description_in => 'Error found @ fusion_pkg.is_coa_valid in line ' ||
                                                  l_line || ' : ' || sqlerrm,
                                object_name_in => 'FUSION_PKG',
                                app_version_in => null,
                                transacted_by_in => null,
                                plant_in => null,
                                source_in => 'CBS_WAM');
  END;

END;