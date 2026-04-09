CREATE OR ALTER VIEW gold.vw_fact_production_enriched AS
SELECT
    fp.production_key,
    dm.machine_id,
    dm.machine_name,
    dm.location,
    dd.full_date,
    dt.full_time,
    fp.source_timestamp,
    dpt.prod_type,
    dpt.product_name,
    dpt.category AS product_category,
    dpt.coffee_amount,
    dpt.water_amount,
    dpt.milk_amount,
    dbh.hopper_code,
    dbh.hopper_name,
    dsr.stop_code,
    dsr.stop_reason,
    CASE fp.outlet_side WHEN 0 THEN 'Left' WHEN 1 THEN 'Right' ELSE 'Unknown' END AS outlet_side_name,
    CASE fp.is_double WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE 'Unknown' END AS is_double_name,
    fp.press_before,
    fp.press_after,
    fp.press_final,
    fp.grind_time_sec,
    fp.extraction_time_sec,
    fp.milk_time_sec,
    fp.water_qnty_ticks,
    fp.water_temp_c,
    fp.milk_temp_c,
    fp.boiler_temp_c,
    fp.steam_pressure_bar,
    fp.grind_adjust_right,
    fp.grind_adjust_left,
    fp.is_coffee_extraction,
    fp.is_milk_event,
    fp.source_file_path,
    fp.source_row_number,
    fp.gold_loaded_utc
FROM gold.fact_production fp
JOIN gold.dim_machine dm ON dm.machine_key = fp.machine_key
JOIN gold.dim_date dd ON dd.date_key = fp.date_key
JOIN gold.dim_time dt ON dt.time_key = fp.time_key
LEFT JOIN gold.dim_product_type dpt ON dpt.product_key = fp.product_key
LEFT JOIN gold.dim_bean_hopper dbh ON dbh.hopper_key = fp.hopper_key
LEFT JOIN gold.dim_stop_reason dsr ON dsr.stop_key = fp.stop_key;
GO

CREATE OR ALTER VIEW gold.vw_fact_cleaning_enriched AS
SELECT
    fc.cleaning_key,
    dm.machine_id,
    dm.machine_name,
    dd.full_date,
    dt.full_time,
    fc.source_timestamp_start,
    fc.source_timestamp_end,
    fc.duration_sec,
    fc.machine_type,
    fc.milk_machine_type,
    ps.status_code AS powder_status_code,
    ps.status_name AS powder_status_name,
    tsl.status_code AS tabs_status_left_code,
    tsl.status_name AS tabs_status_left_name,
    tsr.status_code AS tabs_status_right_code,
    tsr.status_name AS tabs_status_right_name,
    dsl.status_code AS detergent_status_left_code,
    dsl.status_name AS detergent_status_left_name,
    dsr.status_code AS detergent_status_right_code,
    dsr.status_name AS detergent_status_right_name,
    fc.milk_pump_error_left,
    fc.milk_pump_error_right,
    fc.milk_seq_cycle_left_1,
    fc.milk_seq_cycle_left_2,
    fc.milk_seq_cycle_right_1,
    fc.milk_seq_cycle_right_2,
    fc.milk_temp_left_1,
    fc.milk_temp_left_2,
    fc.milk_temp_right_1,
    fc.milk_temp_right_2,
    fc.milk_rpm_left_1,
    fc.milk_rpm_left_2,
    fc.milk_rpm_right_1,
    fc.milk_rpm_right_2,
    fc.source_file_path,
    fc.source_row_number,
    fc.gold_loaded_utc
FROM gold.fact_cleaning fc
JOIN gold.dim_machine dm ON dm.machine_key = fc.machine_key
JOIN gold.dim_date dd ON dd.date_key = fc.date_key
JOIN gold.dim_time dt ON dt.time_key = fc.time_key
LEFT JOIN gold.dim_powder_status ps ON ps.status_key = fc.powder_status_key
LEFT JOIN gold.dim_tabs_status tsl ON tsl.status_key = fc.tabs_status_left_key
LEFT JOIN gold.dim_tabs_status tsr ON tsr.status_key = fc.tabs_status_right_key
LEFT JOIN gold.dim_detergent_status dsl ON dsl.status_key = fc.detergent_status_left_key
LEFT JOIN gold.dim_detergent_status dsr ON dsr.status_key = fc.detergent_status_right_key;
GO

CREATE OR ALTER VIEW gold.vw_fact_rinse_enriched AS
SELECT
    fr.rinse_key,
    dm.machine_id,
    dm.machine_name,
    dd.full_date,
    dt.full_time,
    fr.source_timestamp,
    drt.rinse_code,
    drt.rinse_name,
    drt.description AS rinse_description,
    fsl.status_code AS flow_status_left_code,
    fsl.status_name AS flow_status_left_name,
    fsr.status_code AS flow_status_right_code,
    fsr.status_name AS flow_status_right_name,
    nsl.status_code AS nozzle_status_left_code,
    nsl.status_name AS nozzle_status_left_name,
    nsr.status_code AS nozzle_status_right_code,
    nsr.status_name AS nozzle_status_right_name,
    fr.flow_rate_left,
    fr.flow_rate_right,
    fr.pump_pressure_bar,
    fr.nozzle_flow_rate_left,
    fr.nozzle_flow_rate_right,
    fr.side_active,
    fr.source_file_path,
    fr.source_row_number,
    fr.gold_loaded_utc
FROM gold.fact_rinse fr
JOIN gold.dim_machine dm ON dm.machine_key = fr.machine_key
JOIN gold.dim_date dd ON dd.date_key = fr.date_key
JOIN gold.dim_time dt ON dt.time_key = fr.time_key
LEFT JOIN gold.dim_rinse_type drt ON drt.rinse_type_key = fr.rinse_type_key
LEFT JOIN gold.dim_flow_status fsl ON fsl.status_key = fr.flow_status_left_key
LEFT JOIN gold.dim_flow_status fsr ON fsr.status_key = fr.flow_status_right_key
LEFT JOIN gold.dim_flow_status nsl ON nsl.status_key = fr.nozzle_status_left_key
LEFT JOIN gold.dim_flow_status nsr ON nsr.status_key = fr.nozzle_status_right_key;
GO

CREATE OR ALTER VIEW gold.vw_fact_alerts_enriched AS
SELECT
    fa.alert_key,
    dm.machine_id,
    dm.machine_name,
    dd.full_date,
    dt.full_time,
    fa.source_timestamp,
    dat.alert_code,
    dat.info_message,
    dat.typography,
    dat.type_number,
    dat.severity,
    dat.module,
    dat.is_visible,
    dat.is_recurring,
    dat.is_logged,
    fa.source_file_path,
    fa.source_row_number,
    fa.gold_loaded_utc
FROM gold.fact_alerts fa
JOIN gold.dim_machine dm ON dm.machine_key = fa.machine_key
JOIN gold.dim_date dd ON dd.date_key = fa.date_key
JOIN gold.dim_time dt ON dt.time_key = fa.time_key
LEFT JOIN gold.dim_alert_type dat ON dat.alert_type_key = fa.alert_type_key;
GO
