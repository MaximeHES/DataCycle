MERGE gold.dim_product_type AS tgt
USING (
    SELECT * FROM (VALUES
        (0,   N'None',               N'None',        0,0,0,0,  0.00,   0.00,   0.00),
        (1,   N'Ristretto',          N'Coffee',      0,1,0,0,  7.00,  40.00,   0.00),
        (2,   N'Espresso',           N'Coffee',      0,1,0,0,  7.00,  70.00,   0.00),
        (3,   N'Coffee',             N'Coffee',      0,1,0,0, 10.00,  90.00,   0.00),
        (4,   N'Filter Coffee',      N'Coffee',      0,1,0,0, 12.00,  90.00,   0.00),
        (5,   N'Americano',          N'Coffee',      0,1,0,0,  7.00, 310.00,   0.00),
        (6,   N'Coffee Pot',         N'Coffee',      0,1,0,0, 50.00, 600.00,   0.00),
        (7,   N'Filter Coffee Pot',  N'Coffee',      0,1,0,0, 60.00, 600.00,   0.00),
        (8,   N'Hot Water',          N'Water',       0,0,1,0,  0.00,  90.00,   0.00),
        (9,   N'Manual Steam',       N'Steam',       0,0,0,1,  0.00,   0.00,   0.00),
        (10,  N'Auto Steam',         N'Steam',       0,0,0,1,  0.00,   0.00,   0.00),
        (11,  N'Everfoam',           N'Milk',        1,0,0,0,  0.00,   0.00,   0.00),
        (12,  N'Milk Coffee',        N'Milk Coffee', 1,1,0,0,  7.00,  90.00,  90.00),
        (13,  N'Cappuccino',         N'Milk Coffee', 1,1,0,0,  7.00,  70.00,  50.00),
        (14,  N'Espresso Macchiato', N'Milk Coffee', 1,1,0,0,  7.00,  50.00,  50.00),
        (15,  N'Latte Macchiato',    N'Milk Coffee', 1,1,0,0,  7.00,  60.00, 150.00),
        (16,  N'Milk',               N'Milk',        1,0,0,0,  0.00,   0.00, 200.00),
        (17,  N'Milk Foam',          N'Milk',        1,0,0,0,  0.00,   0.00, 180.00),
        (18,  N'Powder',             N'Powder',      0,0,0,0,  0.00,   0.00,   0.00),
        (19,  N'White Americano',    N'Milk Coffee', 1,1,0,0,  7.00, 100.00,  20.00),
        (255, N'Undef',              N'Unknown',     0,0,0,0,  0.00,   0.00,   0.00)
    ) AS v(prod_type, product_name, category, has_milk, has_coffee, is_hot_water, is_steam, coffee_amount, water_amount, milk_amount)
) AS src
ON tgt.prod_type = src.prod_type
WHEN MATCHED THEN UPDATE SET
    product_name = COALESCE(tgt.product_name, src.product_name),
    category = COALESCE(tgt.category, src.category),
    has_milk = COALESCE(tgt.has_milk, src.has_milk),
    has_coffee = COALESCE(tgt.has_coffee, src.has_coffee),
    is_hot_water = COALESCE(tgt.is_hot_water, src.is_hot_water),
    is_steam = COALESCE(tgt.is_steam, src.is_steam),
    coffee_amount = COALESCE(tgt.coffee_amount, src.coffee_amount),
    water_amount = COALESCE(tgt.water_amount, src.water_amount),
    milk_amount = COALESCE(tgt.milk_amount, src.milk_amount)
WHEN NOT MATCHED THEN
    INSERT (prod_type, product_name, category, has_milk, has_coffee, is_hot_water, is_steam, coffee_amount, water_amount, milk_amount)
    VALUES (src.prod_type, src.product_name, src.category, src.has_milk, src.has_coffee, src.is_hot_water, src.is_steam, src.coffee_amount, src.water_amount, src.milk_amount);
GO

MERGE gold.dim_stop_reason AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'Finished', 0),
        (1, N'Stopped', 0),
        (2, N'Machine Abort', 1),
        (3, N'User Abort', 0)
    ) AS v(stop_code, stop_reason, is_error)
) AS src
ON tgt.stop_code = src.stop_code
WHEN MATCHED THEN UPDATE SET
    stop_reason = COALESCE(tgt.stop_reason, src.stop_reason),
    is_error = COALESCE(tgt.is_error, src.is_error)
WHEN NOT MATCHED THEN
    INSERT (stop_code, stop_reason, is_error)
    VALUES (src.stop_code, src.stop_reason, src.is_error);
GO

MERGE gold.dim_rinse_type AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'InitReboot', N'Rinse triggered at reboot'),
        (1, N'InitWakeup', N'Rinse triggered at wakeup'),
        (2, N'WarmLeft', N'Warm-up rinse left'),
        (3, N'WarmRight', N'Warm-up rinse right'),
        (4, N'AfterClean', N'Rinse after cleaning'),
        (5, N'FlowRate', N'Flow rate rinse'),
        (6, N'RequestedEtc', N'Requested or other rinse'),
        (7, N'Max', N'Technical max code'),
        (255, N'Undef', N'Undefined rinse type')
    ) AS v(rinse_code, rinse_name, description)
) AS src
ON tgt.rinse_code = src.rinse_code
WHEN MATCHED THEN UPDATE SET
    rinse_name = COALESCE(tgt.rinse_name, src.rinse_name),
    description = COALESCE(tgt.description, src.description)
WHEN NOT MATCHED THEN
    INSERT (rinse_code, rinse_name, description)
    VALUES (src.rinse_code, src.rinse_name, src.description);
GO

MERGE gold.dim_flow_status AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'Undef', 0),
        (1, N'Unknown', 0),
        (2, N'TooLow', 0),
        (3, N'TooHigh', 0),
        (4, N'Nozzle05', 1),
        (5, N'Nozzle07', 1),
        (6, N'SystemOk', 1),
        (255, N'Undef', 0)
    ) AS v(status_code, status_name, is_ok)
) AS src
ON tgt.status_code = src.status_code
WHEN MATCHED THEN UPDATE SET
    status_name = COALESCE(tgt.status_name, src.status_name),
    is_ok = COALESCE(tgt.is_ok, src.is_ok)
WHEN NOT MATCHED THEN
    INSERT (status_code, status_name, is_ok)
    VALUES (src.status_code, src.status_name, src.is_ok);
GO

MERGE gold.dim_tabs_status AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'No', 0),(1, N'Yes', 1),(2, N'Undef', 0),(3, N'Error', 0),
        (4, N'Unknown', 0),(5, N'NotNecessary', 1),(6, N'CycleError', 0),(7, N'Max', 0)
    ) AS v(status_code, status_name, is_ok)
) AS src
ON tgt.status_code = src.status_code
WHEN MATCHED THEN UPDATE SET status_name = COALESCE(tgt.status_name, src.status_name), is_ok = COALESCE(tgt.is_ok, src.is_ok)
WHEN NOT MATCHED THEN INSERT (status_code, status_name, is_ok) VALUES (src.status_code, src.status_name, src.is_ok);
GO

MERGE gold.dim_detergent_status AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'Undef', 0),(1, N'No', 0),(2, N'Yes', 1),(3, N'Error', 0),(4, N'Unknown', 0),
        (5, N'NotNecessary', 1),(6, N'CycleAbort', 0),(7, N'CycleWarning', 0),(8, N'DetergentWarning', 0),(9, N'Max', 0)
    ) AS v(status_code, status_name, is_ok)
) AS src
ON tgt.status_code = src.status_code
WHEN MATCHED THEN UPDATE SET status_name = COALESCE(tgt.status_name, src.status_name), is_ok = COALESCE(tgt.is_ok, src.is_ok)
WHEN NOT MATCHED THEN INSERT (status_code, status_name, is_ok) VALUES (src.status_code, src.status_name, src.is_ok);
GO

MERGE gold.dim_powder_status AS tgt
USING (
    SELECT * FROM (VALUES
        (0, N'Undef', NULL),(1, N'NotNecessary', NULL),(2, N'MixerCleaned', NULL),(3, N'WithoutMixer', NULL),(4, N'Max', NULL)
    ) AS v(status_code, status_name, reserved)
) AS src
ON tgt.status_code = src.status_code
WHEN MATCHED THEN UPDATE SET status_name = COALESCE(tgt.status_name, src.status_name), reserved = COALESCE(tgt.reserved, src.reserved)
WHEN NOT MATCHED THEN INSERT (status_code, status_name, reserved) VALUES (src.status_code, src.status_name, src.reserved);
GO

MERGE gold.dim_alert_type AS tgt
USING (
    SELECT * FROM (VALUES
        (N'E-000', N'Bean hopper rear missing.', N'E', 0, N'Low', N'Bean hopper', 1, 0, 1),
        (N'E-001', N'Bean hopper front missing.', N'E', 1, N'Low', N'Bean hopper', 1, 0, 1),
        (N'E-010', N'Software too old. Please start software update.', N'E', 10, N'Critical', N'CPU', 1, 0, 1),
        (N'S-017', N'Cleaning necessary. Please press Continue to start the cleaning.', N'S', 17, N'Low', N'Display / Touch screen', 1, 0, 1),
        (N'S-018', N'The last cleaning hasn''t been finished correctly. Please press Continue to start the cleaning.', N'S', 18, N'Low', N'Display / Touch screen', 1, 0, 1),
        (N'W-013', N'Service necessary.', N'W', 13, N'Medium', N'Machine', 0, 1, 1),
        (N'W-014', N'Please change the water filter.', N'W', 14, N'Medium', N'Water supply', 0, 1, 1)
    ) AS v(alert_code, info_message, typography, type_number, severity, module, is_visible, is_recurring, is_logged)
) AS src
ON tgt.alert_code = src.alert_code
WHEN MATCHED THEN UPDATE SET
    info_message = COALESCE(tgt.info_message, src.info_message),
    typography = COALESCE(tgt.typography, src.typography),
    type_number = COALESCE(tgt.type_number, src.type_number),
    severity = COALESCE(tgt.severity, src.severity),
    module = COALESCE(tgt.module, src.module),
    is_visible = COALESCE(tgt.is_visible, src.is_visible),
    is_recurring = COALESCE(tgt.is_recurring, src.is_recurring),
    is_logged = COALESCE(tgt.is_logged, src.is_logged)
WHEN NOT MATCHED THEN
    INSERT (alert_code, info_message, typography, type_number, severity, module, is_visible, is_recurring, is_logged)
    VALUES (src.alert_code, src.info_message, src.typography, src.type_number, src.severity, src.module, src.is_visible, src.is_recurring, src.is_logged);
GO
