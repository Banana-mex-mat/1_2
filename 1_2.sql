CREATE TABLE logs.log_table (
    log_id SERIAL PRIMARY KEY, 
	log_name TEXT,
    load_start_time TIMESTAMP,
    load_end_time TIMESTAMP,
    additional_info TEXT,
	error_message TEXT
);

SELECT * FROM logs.log_table;

CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8)
);

CREATE TABLE DM.DM_ACCOUNT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8)
);

-- Процедура расчета
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE) 
AS $$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_log_id INT;
    v_error_message TEXT;
BEGIN
    -- Удаляем старые записи за указанную дату
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
	-- Логирование начала
	SELECT NOW() INTO v_start_time;
    INSERT INTO logs.log_table (
	log_name
	, load_start_time
	, additional_info)
    VALUES (
	'fill_account_balance_f'
	, v_start_time
	, 'Начало расчета витрины остатков для даты: ' || i_OnDate)
    RETURNING log_id INTO v_log_id; -- Получение log_id

    BEGIN
	    -- Заполнение данных по кредиту и дебету
        INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
     	    on_date
    	    , account_rk
    	    , credit_amount
	        , credit_amount_rub
    	    , debet_amount
     	    , debet_amount_rub)
        SELECT 
     	    on_date
         	, account_rk
        	, SUM(credit_amount) AS credit_amount
        	, SUM(credit_amount_rub) AS credit_amount_rub
         	, SUM(debet_amount) AS debet_amount
        	, SUM(debet_amount_rub) AS debet_amount_rub
        FROM ( SELECT 
            i_OnDate AS on_date,
            "CREDIT_ACCOUNT_RK" AS account_rk,
            SUM("CREDIT_AMOUNT") AS credit_amount,
            SUM("CREDIT_AMOUNT" * COALESCE(
			(SELECT MAX("REDUCED_COURCE") 
			FROM ds.md_exchange_rate_d 
			WHERE "DATA_ACTUAL_DATE" = i_OnDate), 1)
			) AS credit_amount_rub,
            0 AS debet_amount,
            0 AS debet_amount_rub
        FROM ds.ft_posting_f
        WHERE "OPER_DATE" = i_OnDate
        AND "CREDIT_ACCOUNT_RK" IS NOT NULL
        GROUP BY "CREDIT_ACCOUNT_RK"
        HAVING SUM("CREDIT_AMOUNT") > 0
        UNION ALL
        SELECT 
            i_OnDate AS on_date,
            "DEBET_ACCOUNT_RK" AS account_rk,
            0 AS credit_amount,
            0 AS credit_amount_rub,
            SUM("DEBET_AMOUNT") AS debet_amount,
            SUM("DEBET_AMOUNT" * COALESCE(
			(SELECT MAX("REDUCED_COURCE") 
			FROM ds.md_exchange_rate_d 
			WHERE "DATA_ACTUAL_DATE" = i_OnDate), 1)
			) AS debet_amount_rub
        FROM ds.ft_posting_f
        WHERE "OPER_DATE" = i_OnDate
        AND "DEBET_ACCOUNT_RK" IS NOT NULL
        GROUP BY "DEBET_ACCOUNT_RK"
        HAVING SUM("DEBET_AMOUNT") > 0
    ) AS combined
    GROUP BY on_date, account_rk;
	 -- Логирование окончания
    SELECT NOW() INTO v_end_time;
	UPDATE logs.log_table
    SET load_end_time = v_end_time
    WHERE log_id = v_log_id;
EXCEPTION WHEN OTHERS THEN
    -- Логирование ошибки
    SELECT NOW() INTO v_end_time;
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT; -- Получение текста ошибки
    UPDATE logs.log_table
    SET load_end_time = v_end_time,
        error_message = v_error_message
    WHERE log_id = v_log_id;
	RAISE; -- Перебросить исключение
    END;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    current_date_var DATE := '2018-01-01'; 
BEGIN
    WHILE current_date_var <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(current_date_var);  
        current_date_var := current_date_var + INTERVAL '1 day';  
    END LOOP;
END $$;

-- Необходимо заполнить витрину DM.DM_ACCOUNT_BALANCE_F за 31.12.2017 
-- данными из DS.FT_BALANCE_F
INSERT INTO dm.dm_account_balance_f (
    on_date
	, account_rk
	, balance_out
	, balance_out_rub)
SELECT 
    '2017-12-31' AS on_date,
    "ACCOUNT_RK" AS account_rk,  
    "BALANCE_OUT" AS balance_out,  
    "BALANCE_OUT" * COALESCE(
	    (SELECT "REDUCED_COURCE" 
        FROM ds.md_exchange_rate_d 
        WHERE "DATA_ACTUAL_DATE" = '2017-12-31'), 1) 
		AS balance_out_rub
    FROM DS.FT_BALANCE_F
    WHERE "ON_DATE" = '2017-12-31';  

-- Процедура заполнения витрины остатков по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql AS
$$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_log_id INT;
    v_error_message TEXT;
BEGIN
    -- Логирование начала
    SELECT NOW() INTO v_start_time;
    INSERT INTO logs.log_table (
	log_name
	, load_start_time
	, additional_info)
    VALUES (
	'fill_account_balance_f'
	, v_start_time
	, 'Начало расчета витрины остатков для даты: ' || i_OnDate)
    RETURNING log_id INTO v_log_id;
    BEGIN
        -- Удаляем старые записи за указанную дату
        DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;
        -- Заполнение остатков
        INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
            on_date,
            account_rk,
            balance_out,
            balance_out_rub)
        SELECT 
            i_OnDate,
            a."ACCOUNT_RK",
            CASE 
                WHEN a."CHAR_TYPE" = 'А' THEN 
                    COALESCE(prev.balance_out, 0) 
					+ COALESCE(turnover.debet_amount, 0) 
					- COALESCE(turnover.credit_amount, 0)
                WHEN a."CHAR_TYPE" = 'П' THEN 
                    COALESCE(prev.balance_out, 0) 
					- COALESCE(turnover.debet_amount, 0) 
					+ COALESCE(turnover.credit_amount, 0)
            END AS balance_out,
            CASE 
                WHEN a."CHAR_TYPE" = 'А' THEN 
                    (COALESCE(prev.balance_out, 0) + 
                    COALESCE(turnover.debet_amount_rub, 0) - 
                    COALESCE(turnover.credit_amount_rub, 0))
                WHEN a."CHAR_TYPE" = 'П' THEN 
                    (COALESCE(prev.balance_out, 0) - 
                    COALESCE(turnover.debet_amount_rub, 0) + 
                    COALESCE(turnover.credit_amount_rub, 0))
            END AS balance_out_rub
        FROM DS.MD_ACCOUNT_D a
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F prev 
        ON a."ACCOUNT_RK" = prev.account_rk 
        AND prev.on_date = i_OnDate - INTERVAL '1 day'
        LEFT JOIN (
            SELECT 
                account_rk,
                SUM(debet_amount) AS debet_amount,
                SUM(credit_amount) AS credit_amount,
                SUM(debet_amount_rub) AS debet_amount_rub,
                SUM(credit_amount_rub) AS credit_amount_rub
            FROM DM.DM_ACCOUNT_TURNOVER_F
            WHERE on_date = i_OnDate
            GROUP BY account_rk
        ) turnover ON a."ACCOUNT_RK" = turnover.account_rk
        WHERE a."DATA_ACTUAL_DATE" <= i_OnDate 
        AND (a."DATA_ACTUAL_END_DATE" IS NULL 
        OR a."DATA_ACTUAL_END_DATE" > i_OnDate);

        -- Логирование окончания
        SELECT NOW() INTO v_end_time;
        UPDATE logs.log_table
        SET load_end_time = v_end_time
        WHERE log_id = v_log_id;

    EXCEPTION WHEN OTHERS THEN
        -- Логирование ошибки
        SELECT NOW() INTO v_end_time;
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        UPDATE logs.log_table
        SET load_end_time = v_end_time,
            error_message = v_error_message
        WHERE log_id = v_log_id;
        RAISE;
    END;
END;
$$;

DO $$
DECLARE
    current_date_var DATE := '2018-01-01';  
BEGIN
    WHILE current_date_var <= '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(current_date_var);  
        current_date_var := current_date_var + INTERVAL '1 day';  
    END LOOP;
END $$;