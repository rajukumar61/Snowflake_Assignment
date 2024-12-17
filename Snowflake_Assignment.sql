-- 1. Create roles as per the below-mentioned hierarchy. Accountadmin already
-- exists in Snowflake.

CREATE ROLE ADMIN;
CREATE ROLE DEVELOPER;
CREATE ROLE PII;

GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE DEVELOPER TO ROLE ADMIN;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;

SHOW GRANTS TO ROLE ACCOUNTADMIN;
SHOW GRANTS TO ROLE ADMIN;
SHOW GRANTS TO ROLE DEVELOPER;
SHOW GRANTS TO ROLE PII;

GRANT ROLE ADMIN TO USER BHANUPRASADGANTELA14;
GRANT ROLE DEVELOPER TO USER BHANUPRASADGANTELA14;
GRANT ROLE PII TO USER BHANUPRASADGANTELA14;

drop database ASSIGNMENT_DB;

GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE ADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE ADMIN;

-- 3. Switch to the admin role.
USE ROLE ADMIN;

-- 4. Create a database assignment_db
CREATE DATABASE assignment_db;

-- 5. Create a schema my_schema
CREATE SCHEMA my_schema;

-- 6. Create a table using any sample csv. You can get 1 by googling for sample
-- csv’s. Preferably search for sample employee dataset so that you have PII
-- related columns else you can consider any column as PII.

CREATE TABLE Employees (
    Employee_ID INT PRIMARY KEY,
    First_Name VARCHAR(50),
    Last_Name VARCHAR(50),
    Email VARCHAR(100),
    Phone_Number VARCHAR(50),
    Date_of_Birth DATE,
    SSN VARCHAR(11),
    Address VARCHAR(255),
    elt_ts TIMESTAMP,
    elt_by VARCHAR(255),
    file_name VARCHAR(255)
);

DESC TABLE Employees;

-- 8. Load the file into an external and internal stage.
CREATE OR REPLACE STAGE employees_data_internal_stage;

-- Loading file into the internal stage using SNOWSQL;
-- put file://~/desktop/employee.csv @employees_data_internal_stage;

-- 9. Load data into the tables using copy into statements. In one table load
-- from the internal stage and in another from the external.
COPY INTO Employees
FROM (
   SELECT 
        $1 AS Employee_ID,
        $2 AS First_Name,
        $3 AS Last_name,
        $4 AS Email,
        $5 AS Phone_Number,
        $6 AS Date_of_Birth,
        $7 AS SSN,
        $8 AS Address,
        CURRENT_TIMESTAMP AS elt_ts,
        'Kaggle' AS elt_by,
        METADATA$FILENAME AS file_name -- Captures the file name dynamically
    FROM @ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES_DATA_INTERNAL_STAGE/employee.csv.gz 
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

SELECT * FROM Employees;

-- 7. Also, create a variant version of this dataset.

CREATE OR REPLACE TABLE employees_variant(
employee_data VARIANT
);

INSERT INTO employees_variant(
SELECT TO_VARIANT(OBJECT_CONSTRUCT(*))
FROM Employees
);

SELECT * FROM employees_variant;

CREATE TABLE Employees_external (
    Employee_ID INT PRIMARY KEY,
    First_Name VARCHAR(50),
    Last_Name VARCHAR(50),
    Email VARCHAR(100),
    Phone_Number VARCHAR(50),
    Date_of_Birth DATE,
    SSN VARCHAR(11),
    Address VARCHAR(255),
    elt_ts TIMESTAMP,
    elt_by VARCHAR(255),
    file_name VARCHAR(255)
);

GRANT OWNERSHIP ON STAGE ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES_DATA_INTERNAL_STAGE TO ROLE ADMIN;

GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ADMIN;


CREATE OR REPLACE STORAGE INTEGRATION s3_AWS_integration
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::863518413898:role/Snowflake_Role'
  storage_allowed_locations = ('s3://snowflakebucket92/SF_Bucket/');

DESC INTEGRATION s3_AWS_integration;

CREATE OR REPLACE FILE FORMAT CSVFILEFORMAT
                    type = csv
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;

GRANT USAGE ON INTEGRATION S3_AWS_INTEGRATION TO ROLE ADMIN;

GRANT OWNERSHIP ON INTEGRATION S3_AWS_INTEGRATION TO ROLE ADMIN;


CREATE OR REPLACE STAGE employees_data_external_stage
  URL = 's3://snowflakebucket92/SF_Bucket/'
  STORAGE_INTEGRATION = s3_AWS_integration 
  file_format = CSVFILEFORMAT;

-- Loading the file into the external stage using SNOWSQL;
-- put file://~/desktop/employee.csv @employees_data_internal_stage;

COPY INTO Employees_external
FROM (
   SELECT 
        $1 AS Employee_ID,
        $2 AS First_Name,
        $3 AS Last_name,
        $4 AS Email,
        $5 AS Phone_Number,
        $6 AS Date_of_Birth,
        $7 AS SSN,
        $8 AS Address,
        CURRENT_TIMESTAMP AS elt_ts,
        'Kaggle' AS elt_by,
        METADATA$FILENAME AS file_name -- Captures the file name dynamically
    FROM @ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES_DATA_INTERNAL_STAGE/employee.csv.gz 
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

SELECT * FROM Employees_external;

show stages;

-- 10. Upload any unrelated parquet file to the stage location and infer the schema of the file.

-- run this in SNOWSQL
-- put file://~/downloads/example_test.parquet @employees_data_internal_stage;

CREATE OR REPLACE FILE FORMAT infer_parquet_format
TYPE = PARQUET
COMPRESSION = AUTO
USE_LOGICAL_TYPE = TRUE
TRIM_SPACE = TRUE
REPLACE_INVALID_CHARACTERS = TRUE
NULL_IF = ( '\N', 'NULL', 'NUL', '' );

SELECT * FROM TABLE(INFER_SCHEMA(
 LOCATION=>'@ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES_DATA_INTERNAL_STAGE/example_test.parquet'
 , FILE_FORMAT=>'infer_parquet_format'
 , MAX_RECORDS_PER_FILE => 10));

-- 11.Run a select query on the staged parquet file without loading it to a
-- snowflake table.

SELECT *, 'Kaggle' AS ELT_BY, CURRENT_TIMESTAMP AS ELT_TS, METADATA$FILENAME AS FILE_NAME FROM '@ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES_DATA_INTERNAL_STAGE/example_test.parquet' (FILE_FORMAT => infer_parquet_format);

-- 12. Add masking policy to the PII columns such that fields like email, phone
-- number, etc. show as **masked** to a user with the developer role. If the
-- role is PII the value of these columns should be visible.

CREATE OR REPLACE MASKING POLICY pii_mask AS (val string) RETURNS string ->
CASE
    WHEN current_role() IN ('DEVELOPER') THEN '**masked**'
    ELSE val
END;

ALTER TABLE IF EXISTS Employees MODIFY COLUMN Email SET MASKING POLICY pii_mask;
ALTER TABLE IF EXISTS Employees MODIFY COLUMN Address SET MASKING POLICY pii_mask;
ALTER TABLE IF EXISTS Employees MODIFY COLUMN Phone_Number SET MASKING POLICY pii_mask;

SHOW GRANTS ON TABLE ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEES;

GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE DEVELOPER;
GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE PII;

GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;

GRANT USAGE ON SCHEMA MY_SCHEMA TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA MY_SCHEMA TO ROLE PII;

GRANT SELECT ON TABLE EMPLOYEES TO ROLE DEVELOPER;
GRANT SELECT ON TABLE EMPLOYEES TO ROLE PII;


USE ROLE DEVELOPER;
SELECT * FROM Employees;

USE ROLE PII;
SELECT * FROM Employees;
