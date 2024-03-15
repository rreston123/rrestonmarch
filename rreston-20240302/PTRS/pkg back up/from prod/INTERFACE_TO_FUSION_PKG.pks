CREATE OR REPLACE PACKAGE CBS_FUSION.interface_to_fusion_pkg as

  /*
  Author      :   Albrich L. Sabacajan
  Purpose     :   Interfacing of CBS materials and payables to FUSION
  Overview    :   FUSION VERSION
  
  */

  v_const_contractor constant pls_integer := 1;
  v_const_qa         constant pls_integer := 2;

  v_transaction_type_id        number := 300000004785081; --//W.O Receipt - Excess;
  v_rec_transaction_type_id    number := 300000004785092; --//W.O Receipt - Recovered ;
  v_transaction_source_type_id number := 300000004785079;
  v_lock_status                varchar2(50) := '2';
  v_process_status             varchar2(50) := '3';
  v_cost_component_code        varchar2(50) := 'ITEM_PRICE';

  plant_g varchar2(2) := nvl(apex_util.get_session_state('F_USER_PLANT'),
                             '00');

  procedure interface_excess(p_wonumber  in cbs_wam.staking_transactions.work_order_no%type,
                             p_task_no   in cbs_wam.staking_transactions.work_order_task_no%type,
                             p_groupcode in cbs_wam.staking_transactions.contractor_code%type,
                             p_usertype  in cbs_wam.staking_transactions.user_type%type,
                             plant_in    in cbs_wam.staking_transactions.plant%type);

  procedure interface_excess_qa(p_wonumber           in cbs_wam.staking_transactions.work_order_no%type,
                                p_work_order_task_no in cbs_wam.staking_transactions.work_order_task_no%type,
                                p_transaction_id     in apdu_mtl_issued.transaction_id%type,
                                p_stock_no           in apdu_mtl_issued.stock_no%type,
                                p_serial_number      in apdu_mtl_issued.serial_number%type,
                                p_interface_qty      in number,
                                plant_in             in apdu_mtl_issued.plant%type);

  procedure interface_recovered(p_wonumber  in cbs_wam.staking_transactions.work_order_no%type,
                                p_task_no   in cbs_wam.staking_transactions.work_order_task_no%type,
                                p_groupcode in cbs_wam.staking_transactions.contractor_code%type,
                                p_usertype  in cbs_wam.staking_transactions.user_type%type,
                                plant_in    in cbs_wam.staking_transactions.plant%type);

  procedure interface_recovered_qa(p_wonumber      in cbs_wam.staking_transactions.work_order_no%type,
                                   p_task_no       in cbs_wam.staking_transactions.work_order_task_no%type,
                                   p_stock_no      in apdu_mtl_issued.stock_no%type,
                                   p_interface_qty in number,
                                   plant_in        in cbs_wam.staking_transactions.plant%type);

  procedure interface_to_ap(sar_in        cbs_wam.billings.sar_number%type,
                            contractor_in cbs_wam.billings.contractor_code%type,
                            plant_in      cbs_wam.billings.plant%type);

end interface_to_fusion_pkg;

/
