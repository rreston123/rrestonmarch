CREATE OR REPLACE PACKAGE BODY CBS_FUSION.interface_to_fusion_pkg as

  procedure interface_excess(p_wonumber  in cbs_wam.staking_transactions.work_order_no%type,
                             p_task_no   in cbs_wam.staking_transactions.work_order_task_no%type,
                             p_groupcode in cbs_wam.staking_transactions.contractor_code%type,
                             p_usertype  in cbs_wam.staking_transactions.user_type%type,
                             plant_in    in cbs_wam.staking_transactions.plant%type) is
    /*
      REVISION HISTORY
      v2.0.0 30-July-2018 aocarcallas
        Remarks : If return item is serialize insert data in apdu_mtl_serial_interface table.
    */
    cursor cur_staking_data is
      select stock_desc,
             stock_no,
             move_order,
             line_number,
             actual_cost,
             original_qty,
             qty,
             uom,
             distribution_account_id,
             locator_id,
             subinventory_code,
             organization_id,
             inventory_item_id,
             excess,
             transaction_reference,
             transaction_id,
             work_order_task_no,
             serial_number,
             attribute_category
        from (select b.stock_no,
                     b.doc_number move_order,
                     b.line_number,
                     b.actual_cost,
                     b.original_qty,
                     nvl(a.qty, 0) qty,
                     b.uom,
                     b.distribution_account_id,
                     b.locator_id,
                     b.subinventory_code,
                     b.organization_id,
                     b.inventory_item_id,
                     b.original_qty - nvl(a.qty, 0) excess,
                     b.stock_desc,
                     b.transaction_reference,
                     b.transaction_id,
                     b.work_order_task_no,
                     b.serial_number,
                     b.attribute_category
                from (select st.work_order_no,
                             st.work_order_task_no,
                             st.transaction_source_id transaction_id,
                             st.stock_code,
                             st.serial_number,
                             nvl(sum(st.quantity), 0) qty
                        from cbs_wam.staking_transactions st
                       where st.work_order_no = p_wonumber
                         and st.work_order_task_no = p_task_no
                         and st.contractor_code = p_groupcode
                         and st.user_type = p_usertype
                         and st.transaction_type = 1
                         and st.plant = plant_in
                       group by st.work_order_no,
                                st.work_order_task_no,
                                st.transaction_source_id,
                                st.stock_code,
                                st.serial_number) a,
                     (select mtl.*
                        from (select substr(t.charge_number,
                                            1,
                                            instr(t.charge_number, '-') - 1) wo_number,
                                     substr(t.charge_number,
                                            instr(t.charge_number, '-') + 1) work_order_task_no,
                                     t.*
                                from cbs_fusion.apdu_mtl_issued t
                               where t.plant = plant_in) mtl,
                             cbs_wam.vw_wam_gang_wo vwgw
                       where mtl.wo_number = vwgw.work_order_no
                         and mtl.work_order_task_no = vwgw.work_order_task_no
                         and vwgw.group_code = p_groupcode
                         and mtl.wo_number = p_wonumber
                         and mtl.work_order_task_no = p_task_no
                         and mtl.plant = vwgw.plant) b
               where b.wo_number = a.work_order_no(+)
                 and b.work_order_task_no = a.work_order_task_no(+)
                 and b.transaction_id = a.transaction_id(+)
                 and b.stock_no = a.stock_code(+)
                 and nvl(b.serial_number, 'NULL') =
                     nvl(a.serial_number(+), 'NULL'))
       where excess > 0;
  
    v_excess                      number := 0;
    v_source_line_id              apdu_mtl_txns_interface.source_line_id%type;
    v_ifItemSerialized            varchar2(2) := 'N';
    v_organization_code           apdu_mtl_txns_interface.organization_code%type;
    v_transaction_cost_identifier varchar2(30);
  begin
    for rst_staking_data in cur_staking_data
    loop
      if p_usertype = v_const_qa then
        v_excess := 0;
      else
        v_excess := rst_staking_data.excess;
      end if;
    
      select cbs_fusion.apdumtlinterface_lineid_seq.nextval
        into v_source_line_id
        from dual;
    
      if trim(rst_staking_data.serial_number) is not null then
        v_ifItemSerialized := 'Y';
      else
        v_ifItemSerialized := 'N';
      end if;
    
      select b.organization_code
        into v_organization_code
        from business_units_vw b
       where b.organization_id = rst_staking_data.organization_id;
    
      select 'CBS-' || cbs_fusion.transactioncostidentifier_seq.nextval
        into v_transaction_cost_identifier
        from dual;
    
      insert into cbs_fusion.apdu_mtl_txns_interface
        (source_line_id,
         source_header_id,
         transaction_mode,
         process_flag,
         lock_flag,
         transaction_reference,
         source_code,
         last_update_date,
         last_updated_by,
         creation_date,
         created_by,
         inventory_item_id,
         organization_id,
         transaction_quantity,
         transaction_uom,
         transaction_cost,
         transaction_date,
         subinventory_code,
         locator_id,
         distribution_account_id,
         transaction_type_id,
         reason_id,
         attribute2,
         attribute3,
         attribute4,
         attribute5,
         attribute_category,
         SERIALIZED_ITEM,
         WO_NUMBER,
         WO_TASK_NO,
         ITEM_NUMBER,
         organization_code,
         use_current_cost_flag,
         transaction_cost_identifier,
         transaction_source_type_id,
         lock_status,
         process_status,
         cost_component_code)
      values
        (v_source_line_id,
         cbs_fusion.apdumtlinterface_headerid_seq.nextval,
         3,
         --Default by MD050
         2,
         --Default by MD050
         2,
         --Default by MD050
         p_wonumber || '-' || rst_staking_data.work_order_task_no,
         'Interfaced from CBS Staking',
         --Default by MD050
         sysdate,
         -1,
         --Default by MD050
         sysdate,
         -1,
         --Default by MD050
         rst_staking_data.inventory_item_id,
         rst_staking_data.organization_id,
         v_excess,
         rst_staking_data.uom,
         rst_staking_data.actual_cost,
         sysdate,
         rst_staking_data.subinventory_code,
         rst_staking_data.locator_id,
         rst_staking_data.distribution_account_id,
         v_transaction_type_id,
         -- MD050 Work Order Receipts ? Excess Materials (SRS)
         null,
         '<NONE>',
         rst_staking_data.move_order,
         rst_staking_data.line_number,
         p_wonumber || '-' || rst_staking_data.work_order_task_no,
         rst_staking_data.attribute_category,
         v_ifItemSerialized,
         p_wonumber,
         rst_staking_data.work_order_task_no,
         rst_staking_data.stock_no,
         v_organization_code,
         'false',
         v_transaction_cost_identifier,
         v_transaction_source_type_id,
         v_lock_status,
         v_process_status,
         v_cost_component_code);
    
      if v_ifItemSerialized = 'Y' then
        insert into apdu_mtl_serial_interface
          (source_code,
           source_line_id,
           last_update_date,
           last_updated_by,
           creation_date,
           created_by,
           FM_SERIAL_NUMBER,
           TO_SERIAL_NUMBER)
        values
          ('Interfaced from CBS Staking',
           v_source_line_id,
           sysdate,
           -1,
           sysdate,
           -1,
           rst_staking_data.serial_number,
           rst_staking_data.serial_number);
      end if;
    end loop;
  end interface_excess;

  procedure interface_excess_qa(p_wonumber           in cbs_wam.staking_transactions.work_order_no%type,
                                p_work_order_task_no in cbs_wam.staking_transactions.work_order_task_no%type,
                                p_transaction_id     in apdu_mtl_issued.transaction_id%type,
                                p_stock_no           in apdu_mtl_issued.stock_no%type,
                                p_serial_number      in apdu_mtl_issued.serial_number%type,
                                p_interface_qty      in number,
                                plant_in             in apdu_mtl_issued.plant%type) is
    /*
      REVISION HISTORY
      v2.0.0 30-July-2018 aocarcallas
        Remarks : If return item is serialize insert data in apdu_mtl_serial_interface table.
    */
    cursor wo_mtl_staking_qa is
      select mtl.inventory_item_id,
             mtl.organization_id,
             mtl.stock_no,
             mtl.uom,
             mtl.actual_cost,
             mtl.subinventory_code,
             mtl.locator_id,
             mtl.distribution_account_id,
             mtl.doc_number,
             mtl.line_number,
             mtl.transaction_reference,
             mtl.work_order_task_no,
             mtl.serial_number,
             mtl.attribute_category
        from (select substr(a.charge_number,
                            1,
                            instr(a.charge_number, '-') - 1) wo_number,
                     substr(a.charge_number, instr(a.charge_number, '-') + 1) work_order_task_no,
                     a.*
                from apdu_mtl_issued a) mtl
       where mtl.wo_number = p_wonumber
         and mtl.work_order_task_no = p_work_order_task_no
         and mtl.transaction_id = p_transaction_id
         and mtl.stock_no = p_stock_no
         and nvl(mtl.serial_number, 'NULL') = nvl(p_serial_number, 'NULL')
         and mtl.plant = plant_in;
  
    rwo_mtl_staking_qa            wo_mtl_staking_qa %rowtype;
    v_reference_wo                apdu_mtl_txns_interface.transaction_reference%type;
    v_interface_qty               number;
    v_source_line_id              apdu_mtl_txns_interface.source_line_id%type;
    v_ifItemSerialized            varchar2(2) := 'N';
    v_organization_code           apdu_mtl_txns_interface.organization_code%type;
    v_transaction_cost_identifier varchar2(30);
  begin
    v_reference_wo  := p_wonumber;
    v_interface_qty := p_interface_qty;
  
    for rwo_mtl_staking_qa in wo_mtl_staking_qa
    loop
      select cbs_fusion.apdumtlinterface_lineid_seq.nextval
        into v_source_line_id
        from dual;
    
      if trim(rwo_mtl_staking_qa.serial_number) is not null then
        v_ifItemSerialized := 'Y';
      else
        v_ifItemSerialized := 'N';
      end if;
    
      select b.organization_code
        into v_organization_code
        from business_units_vw b
       where b.organization_id = rwo_mtl_staking_qa.organization_id;
    
      select 'CBS-' || cbs_fusion.transactioncostidentifier_seq.nextval
        into v_transaction_cost_identifier
        from dual;
    
      insert into apdu_mtl_txns_interface
        (source_line_id,
         source_header_id,
         transaction_mode,
         process_flag,
         lock_flag,
         transaction_reference,
         source_code,
         last_update_date,
         last_updated_by,
         creation_date,
         created_by,
         inventory_item_id,
         organization_id,
         transaction_quantity,
         transaction_uom,
         transaction_cost,
         transaction_date,
         subinventory_code,
         locator_id,
         distribution_account_id,
         transaction_type_id,
         reason_id,
         attribute2,
         attribute3,
         attribute4,
         attribute5,
         attribute_category,
         SERIALIZED_ITEM,
         WO_NUMBER,
         WO_TASK_NO,
         ITEM_NUMBER,
         organization_code,
         use_current_cost_flag,
         transaction_cost_identifier,
         transaction_source_type_id,
         lock_status,
         process_status,
         cost_component_code)
      values
        (v_source_line_id,
         cbs_fusion.apdumtlinterface_headerid_seq.nextval,
         3,
         --Default by MD050
         2,
         --Default by MD050
         2,
         --Default by MD050
         v_reference_wo || '-' || rwo_mtl_staking_qa.work_order_task_no,
         'Interfaced from CBS Staking',
         --Default by MD050
         sysdate,
         -1,
         --Default by MD050
         sysdate,
         -1,
         --Default by MD050
         rwo_mtl_staking_qa.inventory_item_id,
         rwo_mtl_staking_qa.organization_id,
         v_interface_qty,
         rwo_mtl_staking_qa.uom,
         rwo_mtl_staking_qa.actual_cost,
         sysdate,
         rwo_mtl_staking_qa.subinventory_code,
         rwo_mtl_staking_qa.locator_id,
         rwo_mtl_staking_qa.distribution_account_id,
         v_transaction_type_id,
         -- MD050 Work Order Receipts ? Excess Materials (SRS)
         null,
         '<NONE>',
         rwo_mtl_staking_qa.doc_number,
         rwo_mtl_staking_qa.line_number,
         p_wonumber || '-' || rwo_mtl_staking_qa.work_order_task_no,
         rwo_mtl_staking_qa.attribute_category,
         v_ifItemSerialized,
         p_wonumber,
         rwo_mtl_staking_qa.work_order_task_no,
         rwo_mtl_staking_qa.stock_no,
         v_organization_code,
         'false',
         v_transaction_cost_identifier,
         v_transaction_source_type_id,
         v_lock_status,
         v_process_status,
         v_cost_component_code);
    
      if v_ifItemSerialized = 'Y' then
        insert into apdu_mtl_serial_interface
          (source_code,
           source_line_id,
           last_update_date,
           last_updated_by,
           creation_date,
           created_by,
           FM_SERIAL_NUMBER,
           TO_SERIAL_NUMBER)
        values
          ('Interfaced from CBS Staking',
           v_source_line_id,
           sysdate,
           -1,
           sysdate,
           -1,
           rwo_mtl_staking_qa.serial_number,
           rwo_mtl_staking_qa.serial_number);
      end if;
    end loop;
  end interface_excess_qa;

  procedure interface_recovered(p_wonumber  in cbs_wam.staking_transactions.work_order_no%type,
                                p_task_no   in cbs_wam.staking_transactions.work_order_task_no%type,
                                p_groupcode in cbs_wam.staking_transactions.contractor_code%type,
                                p_usertype  in cbs_wam.staking_transactions.user_type%type,
                                plant_in    in cbs_wam.staking_transactions.plant%type) is
    cursor cur_staking_data is
      select stock_description,
             stock_code,
             proposed,
             qty,
             uom,
             inventory_item_id,
             interface_qty,
             work_order_task_no,
             organization_id,
             '' attribute_category
        from (select b.stock_code,
                     b.stock_description,
                     b.proposed,
                     nvl(a.qty, 0) qty,
                     b.uom,
                     b.inventory_item_id,
                     nvl(a.qty, 0) interface_qty,
                     b.task_no work_order_task_no,
                     b.organization_id
                from (select st.work_order_no,
                             st.work_order_task_no,
                             st.transaction_source_id transaction_id,
                             st.stock_code,
                             nvl(sum(st.quantity), 0) qty
                        from cbs_wam.staking_transactions st
                       where st.work_order_no = p_wonumber
                         and st.work_order_task_no = p_task_no
                         and st.contractor_code = p_groupcode
                         and st.user_type = p_usertype
                         and st.transaction_type = 2
                         and st.plant = plant_in
                       group by st.work_order_no,
                                st.work_order_task_no,
                                st.transaction_source_id,
                                st.stock_code) a,
                     (select mtl.wo_number,
                             mtl.task_no,
                             mtl.stock_code,
                             mtl.stock_description,
                             mtl.uom,
                             mtl.inventory_item_id,
                             mtl.organization_id,
                             nvl(sum(mtl.proposed), 0) proposed
                        from cbs_wam.view_swmext_proposed_recovrble mtl,
                             cbs_wam.vw_wam_gang_wo                 vwgw
                       where mtl.wo_number = vwgw.work_order_no
                         and mtl.task_no = vwgw.work_order_task_no
                         and mtl.plant = vwgw.plant
                         and vwgw.group_code = p_groupcode
                         and mtl.wo_number = p_wonumber
                         and mtl.task_no = p_task_no
                         and mtl.plant = plant_in
                       group by mtl.wo_number,
                                mtl.task_no,
                                mtl.stock_code,
                                mtl.stock_description,
                                mtl.uom,
                                mtl.inventory_item_id,
                                mtl.organization_id) b
               where b.wo_number = a.work_order_no(+)
                 and b.task_no = a.work_order_task_no(+)
                 and b.stock_code = a.stock_code(+))
       where interface_qty > 0;
  
    v_interface_qty     number := 0;
    v_subinventory_code apdu_mtl_txns_interface.subinventory_code%type;
    v_locator_id        apdu_mtl_txns_interface.locator_id%type;
    v_organization_code apdu_mtl_txns_interface.organization_code%type;
  begin
    begin
      select max(reg.registry_value) keep(dense_rank last order by reg.effective_on) registry_value
        into v_subinventory_code
        from apps_registry reg
       where reg.registry_code = 'REC_MTL_SUBINV'
         and reg.plant = plant_in;
    exception
      when others then
        v_subinventory_code := 'SCRAP';
    end;
  
    begin
      select max(reg.registry_value) keep(dense_rank last order by reg.effective_on) registry_value
        into v_locator_id
        from apps_registry reg
       where reg.registry_code = 'REC_MTL_LOCATOR'
         and reg.plant = plant_in;
    exception
      when others then
        v_locator_id := null;
    end;
  
    for rst_staking_data in cur_staking_data
    loop
      v_interface_qty := rst_staking_data.interface_qty;
    
      select b.organization_code
        into v_organization_code
        from business_units_vw b
       where b.organization_id = rst_staking_data.organization_id;
    
      insert into apdu_mtl_txns_interface
        (source_line_id,
         source_header_id,
         transaction_mode,
         process_flag,
         lock_flag,
         transaction_reference,
         source_code,
         last_update_date,
         last_updated_by,
         creation_date,
         created_by,
         inventory_item_id,
         organization_id,
         transaction_quantity,
         transaction_uom,
         transaction_cost,
         transaction_date,
         subinventory_code,
         locator_id,
         distribution_account_id,
         transaction_type_id,
         reason_id,
         attribute2,
         attribute3,
         attribute4,
         attribute5,
         attribute_category,
         SERIALIZED_ITEM,
         WO_NUMBER,
         WO_TASK_NO,
         ITEM_NUMBER,
         organization_code,
         use_current_cost_flag,
         transaction_source_type_id,
         lock_status,
         process_status,
         cost_component_code)
      values
        (cbs_fusion.apdumtlinterface_lineid_seq.nextval,
         cbs_fusion.apdumtlinterface_headerid_seq.nextval,
         3 /*Default by MD050*/,
         2 /*Default by MD050*/,
         2 /*Default by MD050*/,
         p_wonumber || '-' || rst_staking_data.work_order_task_no,
         'Interface from CBS Recoverable' /*Default by MD050*/,
         sysdate,
         -1 /*Default by MD050*/,
         sysdate,
         -1 /*Default by MD050*/,
         rst_staking_data.inventory_item_id,
         rst_staking_data.organization_id,
         v_interface_qty,
         rst_staking_data.uom,
         null,
         sysdate,
         v_subinventory_code,
         v_locator_id,
         null,
         v_rec_transaction_type_id /*W.O Receipt - Recoverable*/,
         null,
         '<NONE>',
         null,
         null,
         p_wonumber || '-' || rst_staking_data.work_order_task_no,
         rst_staking_data.attribute_category,
         'N',
         p_wonumber,
         rst_staking_data.work_order_task_no,
         rst_staking_data.stock_code,
         v_organization_code,
         'true',
         v_transaction_source_type_id,
         v_lock_status,
         v_process_status,
         v_cost_component_code);
    end loop;
  end interface_recovered;

  procedure interface_recovered_qa(p_wonumber      in cbs_wam.staking_transactions.work_order_no%type,
                                   p_task_no       in cbs_wam.staking_transactions.work_order_task_no%type,
                                   p_stock_no      in apdu_mtl_issued.stock_no%type,
                                   p_interface_qty in number,
                                   plant_in        in cbs_wam.staking_transactions.plant%type) is
  
    cursor wo_mtl_staking_qa is
      select distinct rec.inventory_item_id,
                      rec.uom,
                      rec.stock_code,
                      rec.task_no,
                      rec.organization_id,
                      '' attribute_category
        from cbs_wam.view_swmext_proposed_recovrble rec
       where rec.wo_number = p_wonumber
         and rec.task_no = p_task_no
         and rec.stock_code = p_stock_no
         and rec.plant = plant_in;
  
    rwo_mtl_staking_qa  wo_mtl_staking_qa %rowtype;
    v_reference_wo      apdu_mtl_txns_interface.transaction_reference%type;
    v_interface_qty     number;
    v_subinventory_code apdu_mtl_txns_interface.subinventory_code%type;
    v_locator_id        apdu_mtl_txns_interface.locator_id%type;
    v_organization_code apdu_mtl_txns_interface.organization_code%type;
  begin
    v_reference_wo  := p_wonumber;
    v_interface_qty := p_interface_qty;
  
    begin
      select max(reg.registry_value) keep(dense_rank last order by reg.effective_on) registry_value
        into v_subinventory_code
        from apps_registry reg
       where reg.registry_code = 'REC_MTL_SUBINV'
         and reg.plant = plant_in;
    exception
      when others then
        v_subinventory_code := 'SCRAP';
    end;
  
    begin
      select max(reg.registry_value) keep(dense_rank last order by reg.effective_on) registry_value
        into v_locator_id
        from apps_registry reg
       where reg.registry_code = 'REC_MTL_LOCATOR'
         and reg.plant = plant_in;
    exception
      when others then
        v_locator_id := null;
    end;
  
    for rwo_mtl_staking_qa in wo_mtl_staking_qa
    loop
      select b.organization_code
        into v_organization_code
        from business_units_vw b
       where b.organization_id = rwo_mtl_staking_qa.organization_id;
    
      insert into apdu_mtl_txns_interface
        (source_line_id,
         source_header_id,
         transaction_mode,
         process_flag,
         lock_flag,
         transaction_reference,
         source_code,
         last_update_date,
         last_updated_by,
         creation_date,
         created_by,
         inventory_item_id,
         organization_id,
         transaction_quantity,
         transaction_uom,
         transaction_cost,
         transaction_date,
         subinventory_code,
         locator_id,
         distribution_account_id,
         transaction_type_id,
         reason_id,
         attribute2,
         attribute3,
         attribute4,
         attribute5,
         attribute_category,
         SERIALIZED_ITEM,
         WO_NUMBER,
         WO_TASK_NO,
         ITEM_NUMBER,
         organization_code,
         use_current_cost_flag,
         transaction_source_type_id,
         lock_status,
         process_status,
         cost_component_code)
      values
        (cbs_fusion.apdumtlinterface_lineid_seq.nextval,
         cbs_fusion.apdumtlinterface_headerid_seq.nextval,
         3 /*Default by MD050 */,
         2 /*Default by MD050 */,
         2 /*Default by MD050 */,
         v_reference_wo || '-' || rwo_mtl_staking_qa.task_no,
         'Interface from CBS Recoverable' /*Default by MD050 */,
         sysdate,
         -1 /*Default by MD050 */,
         sysdate,
         -1 /*Default by MD050 */,
         rwo_mtl_staking_qa.inventory_item_id,
         rwo_mtl_staking_qa.organization_id,
         v_interface_qty,
         rwo_mtl_staking_qa.uom,
         null,
         sysdate,
         v_subinventory_code,
         v_locator_id,
         null,
         v_rec_transaction_type_id /*W.O Receipt - Recoverable*/,
         null,
         '<NONE>',
         null,
         null,
         p_wonumber || '-' || rwo_mtl_staking_qa.task_no,
         rwo_mtl_staking_qa.attribute_category,
         'N',
         p_wonumber,
         rwo_mtl_staking_qa.task_no,
         rwo_mtl_staking_qa.stock_code,
         v_organization_code,
         'true',
         v_transaction_source_type_id,
         v_lock_status,
         v_process_status,
         v_cost_component_code);
    end loop;
  end interface_recovered_qa;

  procedure interface_to_ap(sar_in        cbs_wam.billings.sar_number%type,
                            contractor_in cbs_wam.billings.contractor_code%type,
                            plant_in      cbs_wam.billings.plant%type) is
  
    cursor ap_lines_cur is
      select work_order_no,
             work_order_task_no,
             bt.request_number,
             replace(gl, '.', '-') gl,
             sum(bt.amount) amount
        from (select bt.plant,
                     rd.request_number,
                     bt.bill_tag_amount amount,
                     dc.wo_number work_order_no,
                     decode(dc.direct_charge,
                            null,
                            dc.wo_taskno,
                            dc.direct_charge) work_order_task_no,
                     --dc.direct_charge work_order_task_no,
                     (substr(account_no, 1, 5) || expense_code ||
                     substr(account_no, 5)) gl
                from cbs_wam.billing_tags                bt,
                     cbs_wam.request_details             rd,
                     cbs_wam.work_order_service_contract dc
               where bt.plant = rd.plant
                 and bt.plant = dc.plant
                 and bt.wo_number = rd.wo_number
                 and bt.wo_taskno = rd.wo_taskno
                 and bt.wo_number = substr(dc.wo_number, 2)
                 and bt.wo_taskno = dc.wo_taskno
                 and bt.bill_tag_number = rd.bill_tag_number) bt,
             cbs_wam.billings bill,
             cbs_wam.gang_groups_lib ggl
       where bt.plant = bill.plant
         and bill.plant = ggl.plant
         and bt.request_number = bill.request_number
         and bill.contractor_code = ggl.code
         and bill.sar_number = sar_in
         and bill.contractor_code = contractor_in
         and bt.plant = plant_in
       group by work_order_no,
                work_order_task_no,
                bt.request_number,
                gl;
  
    supplierID         cbs_wam.erp_mapping_po_vendors.fusion_supplier_ID%type;
    suppliersiteID     cbs_wam.erp_mapping_po_vendors.fusion_suppliersite_ID%type;
    supplierName       cbs_fusion.fusion_suppliers.SUPPLIER_NAME%type;
    suppliersiteName   cbs_fusion.fusion_suppliers.SUPPLIER_SITE_NAME%type;
    business_unit_id   number;
    business_unit_name varchar2(1000);
  
    v_invoice         apdu_ap_inv_interface.invoice_num%type := 'CBS_WAM-' ||
                                                                sar_in;
    org_id_l          number;
    line_number_l     apdu_ap_inv_lines_interface.line_number%type;
    invoice_id_l      apdu_ap_inv_interface.invoice_id%type;
    invoice_line_id_l apdu_ap_inv_lines_interface.invoice_line_id%type;
    amount_l          apdu_ap_inv_interface.invoice_amount%type;
    invoice_l         apdu_ap_inv_interface.invoice_num%type;
    activity_l        varchar2(4000);
  begin
    activity_l := 'selecting supplier';
    --FUSION
    select FUSION_SUPPLIER_ID,
           FUSION_SUPPLIERSITE_ID,
           fusion_supplier,
           fusion_suppliersite
      into supplierID,
           suppliersiteID,
           supplierName,
           suppliersiteName
      from cbs_wam.erp_mapping_po_vendors
     where cbs_value = contractor_in
       and plant = plant_in;
  
    activity_l := 'selecting organization id';
    --use for FUSION
    select business_unit_id,
           business_unit_name,
           organization_id
      into business_unit_id,
           business_unit_name,
           org_id_l
      from cbs_fusion.business_units_vw
     where rownum = 1
       and plant = plant_in;
  
    activity_l := 'selecting total amount';
    select sum(bt.amount),
           si.invoice_number
      into amount_l,
           invoice_l
      from (select bt.plant,
                   rd.request_number,
                   bt.bill_tag_amount amount
              from cbs_wam.billing_tags                bt,
                   cbs_wam.request_details             rd,
                   cbs_wam.work_order_service_contract dc
             where bt.plant = rd.plant
               and bt.plant = dc.plant
               and bt.wo_number = rd.wo_number
               and bt.wo_taskno = rd.wo_taskno
               and bt.wo_number = substr(dc.wo_number, 2)
               and bt.wo_taskno = dc.wo_taskno
               and bt.bill_tag_number = rd.bill_tag_number) bt,
           cbs_wam.billings bill,
           cbs_wam.gang_groups_lib ggl,
           cbs_wam.sar_invoice si
     where bt.plant = bill.plant
       and bill.plant = ggl.plant
       and bill.plant = si.plant
       and bt.request_number = bill.request_number
       and bill.contractor_code = ggl.code
       and bill.contractor_code = si.contractor_code
       and bill.sar_number = si.sar_number
       and bill.sar_number = sar_in
       and bill.contractor_code = contractor_in
       and bt.plant = plant_in
     group by invoice_number;
  
    activity_l := 'selecting apdu_ap_invoice_id_seq';
    select apdu_ap_invoice_id_seq.nextval
      into invoice_id_l
      from sys.dual;
  
    update cbs_wam.sar_invoice
       set forwarded = 2 -- interfaced
     where contractor_code = contractor_in
       and sar_number = sar_in
       and plant = plant_in;
  
    insert into apdu_ap_inv_interface
      (invoice_id,
       invoice_num,
       supplier_id,
       supplier_site_id,
       invoice_amount,
       org_id,
       source,
       invoice_date,
       invoice_type_lookup_code,
       invoice_currency_code,
       terms_id,
       gl_date,
       invoice_received_date,
       creation_date,
       calc_tax_during_import_flag,
       taxation_country,
       attribute1,
       description,
       supplier_name,
       supplier_site_name,
       business_unit_id,
       business_unit_name)
    values
      (invoice_id_l,
       v_invoice,
       supplierID,
       suppliersiteID,
       amount_l,
       org_id_l,
       'APDU_CBS',
       sysdate,
       'STANDARD',
       'PHP',
       '',
       sysdate,
       sysdate,
       sysdate,
       'Y',
       'PH',
       'LABOR',
       'INVOICE # ' || invoice_l || ' - CBS_WAM',
       supplierName,
       suppliersiteName,
       business_unit_id,
       business_unit_name);
  
    line_number_l := 1;
  
    activity_l := 'looping cursor ap_lines_cur';
    for ap_lines_rst in ap_lines_cur
    loop
      activity_l := 'selecting apdu_ap_invoice_lines_id_seq';
      select cbs_fusion.apdu_ap_invoice_lines_id_seq.nextval
        into invoice_line_id_l
        from sys.dual;
    
      activity_l := 'inserting data apdu_ap_inv_lines_interface';
      insert into apdu_ap_inv_lines_interface
        (invoice_id,
         invoice_line_id,
         line_number,
         line_type_lookup_code,
         amount,
         accounting_date,
         dist_code_concatenated,
         description,
         user_defined_fisc_class,
         attribute1,
         attribute2,
         attribute3)
      values
        (invoice_id_l,
         invoice_line_id_l,
         line_number_l,
         'ITEM',
         ap_lines_rst.amount,
         sysdate,
         ap_lines_rst.gl,
         ap_lines_rst.work_order_no || '-' ||
         ap_lines_rst.work_order_task_no,
         'SERVICE',
         ap_lines_rst.work_order_no,
         ap_lines_rst.work_order_task_no,
         'CONTRACT');
    
      line_number_l := line_number_l + 1;
    
    end loop;
  
    commit;
  
  exception
    when others then
      rollback;
      raise_application_error(-20003,
                              'Please contact ServiceDesk about this error ' ||
                              dbms_utility.format_error_stack || ' / ' ||
                              activity_l);
  end interface_to_ap;

end interface_to_fusion_pkg;

/
