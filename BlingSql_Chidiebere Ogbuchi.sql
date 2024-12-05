-- DROP TABLE IF EXISTS bling_accounts;

-- CREATE TABLE bling_accounts (
--     bba_id TEXT,                    -- UUID string identifier for each bank account
--     bba_balance FLOAT,              -- Balance as a floating-point number
--     bba_updated_at TIMESTAMPTZ      -- Timestamp with timezone for the balance update
-- );

-- --Change datapath and use copy if on pgadmin
-- \copy bling_accounts (bba_id, bba_balance, bba_updated_at) FROM 'Your File Path' DELIMITER ',' CSV HEADER;


-- Step 1: Filter daily duplicates to keep the latest balance for each bba_id and day, and create datekey
WITH latest_daily_balance AS (
    SELECT 
        bba_id,
        bba_updated_at::date AS date,  -- Convert timestamp to date
        bba_balance,
        ROW_NUMBER() OVER (PARTITION BY bba_id, bba_updated_at::date ORDER BY bba_updated_at DESC) AS row_num  -- Assign row number based on date
    FROM bling_accounts
),

-- Step 2: Keep only the latest entry per day for each bba_id
filtered_balance AS (
    SELECT 
        bba_id, 
        date, 
        bba_balance  -- Include datekey here
    FROM latest_daily_balance
    WHERE row_num = 1  -- Filter to keep only the latest entry
),

-- Step 3: Create a date range for all dates from the minimum to maximum date in filtered_balance
date_range AS (
    SELECT 
        generate_series(
            (SELECT MIN(date) FROM filtered_balance),  -- Minimum date
            (SELECT MAX(date) FROM filtered_balance),  -- Maximum date
            '1 day'::interval  -- Increment by one day
        )::date AS date  -- Cast to date type
),

-- Step 4: Get all combinations of bba_id and each date in the range
all_dates_per_account AS (
    SELECT DISTINCT 
        f.bba_id, 
        d.date
    FROM 
        (SELECT DISTINCT bba_id FROM filtered_balance) AS f  -- Distinct bba_ids
    CROSS JOIN 
        date_range AS d  -- Cross join to create all combinations of bba_id and dates
),

-- Step 5: Left join to bring in the existing balances for each bba_id and date
joined_data AS (
    SELECT 
        a.bba_id, 
        a.date, 
        f.bba_balance  -- Include balance for matching bba_id and date
    FROM 
        all_dates_per_account AS a
    LEFT JOIN 
        filtered_balance AS f
    ON 
        a.bba_id = f.bba_id AND a.date = f.date  -- Match on bba_id and date
),

-- Step 6: Create a grouping for forward fill
reconstitution_data AS (
    SELECT 
        *, 
        COUNT(bba_balance) OVER (PARTITION BY bba_id ORDER BY date) AS Grp  -- Create group based on bba_id and order by date
    FROM 
        joined_data
)

-- Step 7: Final selection with forward filling of balances
SELECT 
    bba_id, 
    date, 
    bba_balance, 
    FIRST_VALUE(bba_balance) OVER (PARTITION BY bba_id, Grp ORDER BY date) AS bba_balance_fill  -- Forward fill the balances
FROM 
    reconstitution_data;
