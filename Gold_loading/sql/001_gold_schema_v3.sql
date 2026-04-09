IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

IF OBJECT_ID('gold.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_date (
        date_key INT NOT NULL PRIMARY KEY,
        full_date DATE NOT NULL,
        [year] SMALLINT NOT NULL,
        [month] TINYINT NOT NULL,
        month_name NVARCHAR(20) NOT NULL,
        quarter TINYINT NOT NULL,
        week_iso TINYINT NOT NULL,
        day_of_month TINYINT NOT NULL,
        day_of_week TINYINT NOT NULL,
        day_name NVARCHAR(20) NOT NULL,
        is_weekend BIT NOT NULL
    );
END
GO

IF OBJECT_ID('gold.dim_time', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_time (
        time_key INT NOT NULL PRIMARY KEY,
        full_time TIME(0) NOT NULL,
        [hour] TINYINT NOT NULL,
        [minute] TINYINT NOT NULL,
        am_pm CHAR(2) NOT NULL,
        five_min_group TIME(0) NULL,
        thirty_min_group TIME(0) NULL,
        sixty_min_group TIME(0) NULL
    );

    ;WITH n AS (
        SELECT TOP (1440) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS m
        FROM sys.all_objects
    )
    INSERT INTO gold.dim_time (time_key, full_time, [hour], [minute], am_pm, five_min_group, thirty_min_group, sixty_min_group)
    SELECT
        (m / 60) * 100 + (m % 60),
        TIMEFROMPARTS(m / 60, m % 60, 0, 0, 0),
        m / 60,
        m % 60,
        CASE WHEN m / 60 < 12 THEN 'AM' ELSE 'PM' END,
        TIMEFROMPARTS(m / 60, (m / 5) * 5 % 60, 0, 0, 0),
        TIMEFROMPARTS(m / 60, (m / 30) * 30 % 60, 0, 0, 0),
        TIMEFROMPARTS(m / 60, 0, 0, 0, 0)
    FROM n;
END
GO

IF OBJECT_ID('gold.dim_machine', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_machine (
        machine_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        machine_id INT NOT NULL,
        machine_name NVARCHAR(200) NULL,
        [location] NVARCHAR(200) NULL,
        model NVARCHAR(200) NULL,
        install_date DATE NULL,
        is_active BIT NOT NULL CONSTRAINT DF_dim_machine_is_active DEFAULT (1),
        valid_from DATETIME2(0) NOT NULL,
        valid_to DATETIME2(0) NULL,
        is_current BIT NOT NULL CONSTRAINT DF_dim_machine_is_current DEFAULT (1)
    );
    CREATE UNIQUE INDEX UX_dim_machine_machine_id_current
        ON gold.dim_machine(machine_id)
        WHERE is_current = 1;
END
GO

IF OBJECT_ID('gold.dim_product_type', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_product_type (
        product_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        prod_type INT NOT NULL UNIQUE,
        product_name NVARCHAR(200) NULL,
        category NVARCHAR(100) NULL,
        has_milk BIT NULL,
        has_coffee BIT NULL,
        is_hot_water BIT NULL,
        is_steam BIT NULL,
        coffee_amount DECIMAL(10,2) NULL,
        water_amount DECIMAL(10,2) NULL,
        milk_amount DECIMAL(10,2) NULL
    );
END
GO

IF OBJECT_ID('gold.dim_bean_hopper', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_bean_hopper (
        hopper_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        hopper_code INT NOT NULL UNIQUE,
        hopper_name NVARCHAR(200) NULL,
        position NVARCHAR(50) NULL
    );
END
GO

IF OBJECT_ID('gold.dim_stop_reason', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_stop_reason (
        stop_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        stop_code INT NOT NULL UNIQUE,
        stop_reason NVARCHAR(200) NULL,
        is_error BIT NULL
    );
END
GO

IF OBJECT_ID('gold.dim_rinse_type', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_rinse_type (
        rinse_type_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        rinse_code INT NOT NULL UNIQUE,
        rinse_name NVARCHAR(200) NULL,
        description NVARCHAR(500) NULL
    );
END
GO

IF OBJECT_ID('gold.dim_flow_status', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_flow_status (
        status_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        status_code INT NOT NULL UNIQUE,
        status_name NVARCHAR(200) NULL,
        is_ok BIT NULL
    );
END
GO

IF OBJECT_ID('gold.dim_tabs_status', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_tabs_status (
        status_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        status_code INT NOT NULL UNIQUE,
        status_name NVARCHAR(200) NULL,
        is_ok BIT NULL
    );
END
GO

IF OBJECT_ID('gold.dim_detergent_status', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_detergent_status (
        status_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        status_code INT NOT NULL UNIQUE,
        status_name NVARCHAR(200) NULL,
        is_ok BIT NULL
    );
END
GO

IF OBJECT_ID('gold.dim_powder_status', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_powder_status (
        status_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        status_code INT NOT NULL UNIQUE,
        status_name NVARCHAR(200) NULL,
        reserved NVARCHAR(20) NULL
    );
END
GO

IF OBJECT_ID('gold.dim_alert_type', 'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_alert_type (
        alert_type_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        alert_code NVARCHAR(20) NOT NULL UNIQUE,
        info_message NVARCHAR(500) NULL,
        typography NVARCHAR(10) NULL,
        type_number INT NULL,
        severity NVARCHAR(50) NULL,
        module NVARCHAR(100) NULL,
        is_visible BIT NULL,
        is_recurring BIT NULL,
        is_logged BIT NULL
    );
END
GO

IF OBJECT_ID('gold.fact_production', 'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_production (
        production_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        machine_key INT NOT NULL,
        date_key INT NOT NULL,
        time_key INT NOT NULL,
        product_key INT NULL,
        hopper_key INT NULL,
        stop_key INT NULL,
        source_timestamp DATETIME2(0) NOT NULL,
        outlet_side TINYINT NULL,
        is_double BIT NULL,
        press_before FLOAT NULL,
        press_after FLOAT NULL,
        press_final FLOAT NULL,
        grind_time_sec FLOAT NULL,
        extraction_time_sec FLOAT NULL,
        milk_time_sec FLOAT NULL,
        water_qnty_ticks FLOAT NULL,
        water_temp_c FLOAT NULL,
        milk_temp_c FLOAT NULL,
        boiler_temp_c FLOAT NULL,
        steam_pressure_bar FLOAT NULL,
        grind_adjust_right FLOAT NULL,
        grind_adjust_left FLOAT NULL,
        is_coffee_extraction BIT NULL,
        is_milk_event BIT NULL,
        source_file_path NVARCHAR(500) NOT NULL,
        source_row_number INT NOT NULL,
        source_row_hash CHAR(64) NOT NULL,
        gold_loaded_utc DATETIME2(0) NOT NULL CONSTRAINT DF_fact_prod_gold_loaded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_fact_prod_machine FOREIGN KEY (machine_key) REFERENCES gold.dim_machine(machine_key),
        CONSTRAINT FK_fact_prod_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_prod_time FOREIGN KEY (time_key) REFERENCES gold.dim_time(time_key),
        CONSTRAINT FK_fact_prod_product FOREIGN KEY (product_key) REFERENCES gold.dim_product_type(product_key),
        CONSTRAINT FK_fact_prod_hopper FOREIGN KEY (hopper_key) REFERENCES gold.dim_bean_hopper(hopper_key),
        CONSTRAINT FK_fact_prod_stop FOREIGN KEY (stop_key) REFERENCES gold.dim_stop_reason(stop_key)
    );
    CREATE UNIQUE INDEX UX_fact_production_hash ON gold.fact_production(source_row_hash);
    CREATE INDEX IX_fact_production_machine_date ON gold.fact_production(machine_key, date_key, time_key);
END
GO

IF OBJECT_ID('gold.fact_cleaning', 'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_cleaning (
        cleaning_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        machine_key INT NOT NULL,
        date_key INT NOT NULL,
        time_key INT NOT NULL,
        powder_status_key INT NULL,
        tabs_status_left_key INT NULL,
        tabs_status_right_key INT NULL,
        detergent_status_left_key INT NULL,
        detergent_status_right_key INT NULL,
        source_timestamp_start DATETIME2(0) NULL,
        source_timestamp_end DATETIME2(0) NULL,
        duration_sec INT NULL,
        machine_type NVARCHAR(30) NULL,
        milk_machine_type NVARCHAR(30) NULL,
        milk_pump_error_left FLOAT NULL,
        milk_pump_error_right FLOAT NULL,
        milk_seq_cycle_left_1 FLOAT NULL,
        milk_seq_cycle_left_2 FLOAT NULL,
        milk_seq_cycle_right_1 FLOAT NULL,
        milk_seq_cycle_right_2 FLOAT NULL,
        milk_temp_left_1 FLOAT NULL,
        milk_temp_left_2 FLOAT NULL,
        milk_temp_right_1 FLOAT NULL,
        milk_temp_right_2 FLOAT NULL,
        milk_rpm_left_1 FLOAT NULL,
        milk_rpm_left_2 FLOAT NULL,
        milk_rpm_right_1 FLOAT NULL,
        milk_rpm_right_2 FLOAT NULL,
        source_file_path NVARCHAR(500) NOT NULL,
        source_row_number INT NOT NULL,
        source_row_hash CHAR(64) NOT NULL,
        gold_loaded_utc DATETIME2(0) NOT NULL CONSTRAINT DF_fact_clean_gold_loaded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_fact_clean_machine FOREIGN KEY (machine_key) REFERENCES gold.dim_machine(machine_key),
        CONSTRAINT FK_fact_clean_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_clean_time FOREIGN KEY (time_key) REFERENCES gold.dim_time(time_key),
        CONSTRAINT FK_fact_clean_powder FOREIGN KEY (powder_status_key) REFERENCES gold.dim_powder_status(status_key),
        CONSTRAINT FK_fact_clean_tabs_left FOREIGN KEY (tabs_status_left_key) REFERENCES gold.dim_tabs_status(status_key),
        CONSTRAINT FK_fact_clean_tabs_right FOREIGN KEY (tabs_status_right_key) REFERENCES gold.dim_tabs_status(status_key),
        CONSTRAINT FK_fact_clean_det_left FOREIGN KEY (detergent_status_left_key) REFERENCES gold.dim_detergent_status(status_key),
        CONSTRAINT FK_fact_clean_det_right FOREIGN KEY (detergent_status_right_key) REFERENCES gold.dim_detergent_status(status_key)
    );
    CREATE UNIQUE INDEX UX_fact_cleaning_hash ON gold.fact_cleaning(source_row_hash);
    CREATE INDEX IX_fact_cleaning_machine_date ON gold.fact_cleaning(machine_key, date_key, time_key);
END
GO

IF OBJECT_ID('gold.fact_rinse', 'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_rinse (
        rinse_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        machine_key INT NOT NULL,
        date_key INT NOT NULL,
        time_key INT NOT NULL,
        rinse_type_key INT NULL,
        flow_status_left_key INT NULL,
        flow_status_right_key INT NULL,
        nozzle_status_left_key INT NULL,
        nozzle_status_right_key INT NULL,
        source_timestamp DATETIME2(0) NOT NULL,
        flow_rate_left FLOAT NULL,
        flow_rate_right FLOAT NULL,
        pump_pressure_bar FLOAT NULL,
        nozzle_flow_rate_left FLOAT NULL,
        nozzle_flow_rate_right FLOAT NULL,
        side_active NVARCHAR(10) NULL,
        source_file_path NVARCHAR(500) NOT NULL,
        source_row_number INT NOT NULL,
        source_row_hash CHAR(64) NOT NULL,
        gold_loaded_utc DATETIME2(0) NOT NULL CONSTRAINT DF_fact_rinse_gold_loaded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_fact_rinse_machine FOREIGN KEY (machine_key) REFERENCES gold.dim_machine(machine_key),
        CONSTRAINT FK_fact_rinse_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_rinse_time FOREIGN KEY (time_key) REFERENCES gold.dim_time(time_key),
        CONSTRAINT FK_fact_rinse_type FOREIGN KEY (rinse_type_key) REFERENCES gold.dim_rinse_type(rinse_type_key),
        CONSTRAINT FK_fact_rinse_flow_left FOREIGN KEY (flow_status_left_key) REFERENCES gold.dim_flow_status(status_key),
        CONSTRAINT FK_fact_rinse_flow_right FOREIGN KEY (flow_status_right_key) REFERENCES gold.dim_flow_status(status_key),
        CONSTRAINT FK_fact_rinse_nozzle_left FOREIGN KEY (nozzle_status_left_key) REFERENCES gold.dim_flow_status(status_key),
        CONSTRAINT FK_fact_rinse_nozzle_right FOREIGN KEY (nozzle_status_right_key) REFERENCES gold.dim_flow_status(status_key)
    );
    CREATE UNIQUE INDEX UX_fact_rinse_hash ON gold.fact_rinse(source_row_hash);
    CREATE INDEX IX_fact_rinse_machine_date ON gold.fact_rinse(machine_key, date_key, time_key);
END
GO

IF OBJECT_ID('gold.fact_alerts', 'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_alerts (
        alert_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        machine_key INT NOT NULL,
        date_key INT NOT NULL,
        time_key INT NOT NULL,
        alert_type_key INT NULL,
        source_timestamp DATETIME2(0) NOT NULL,
        source_file_path NVARCHAR(500) NOT NULL,
        source_row_number INT NOT NULL,
        source_row_hash CHAR(64) NOT NULL,
        gold_loaded_utc DATETIME2(0) NOT NULL CONSTRAINT DF_fact_alerts_gold_loaded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_fact_alerts_machine FOREIGN KEY (machine_key) REFERENCES gold.dim_machine(machine_key),
        CONSTRAINT FK_fact_alerts_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_alerts_time FOREIGN KEY (time_key) REFERENCES gold.dim_time(time_key),
        CONSTRAINT FK_fact_alerts_type FOREIGN KEY (alert_type_key) REFERENCES gold.dim_alert_type(alert_type_key)
    );
    CREATE UNIQUE INDEX UX_fact_alerts_hash ON gold.fact_alerts(source_row_hash);
    CREATE INDEX IX_fact_alerts_machine_date ON gold.fact_alerts(machine_key, date_key, time_key);
END
GO
