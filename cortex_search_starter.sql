create table test1 as select 1 as col1;

drop table test1;

create or replace stage policy_documents encryption = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE);


ls @policy_documents;


-- We use layout mode to attempt to extract text based on markdown 
-- You could use OCR mod e
-- You could also use Python scripting
CREATE or replace TEMPORARY table RAW_TEXT AS
SELECT 
    RELATIVE_PATH,
    SIZE,
    FILE_URL,
    build_scoped_file_url(@policy_documents, relative_path) as scoped_file_url,
    TO_VARCHAR (
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT (
            '@policy_documents',
            RELATIVE_PATH,
            {'mode': 'LAYOUT'} ):content --Layout mode is a
        ) AS EXTRACTED_LAYOUT 
FROM 
    DIRECTORY('@policy_documents');


select * from RAW_TEXT;


create or replace TABLE POLICY_DOCS_CHUNKS ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CHUNK_INDEX INTEGER, -- Index for the text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);





insert into POLICY_DOCS_CHUNKS (relative_path, size, file_url,
                            scoped_file_url, chunk, chunk_index)

    select relative_path, 
            size,
            file_url, 
            scoped_file_url,
            c.value::TEXT as chunk,
            c.INDEX::INTEGER as chunk_index
            
    from 
        raw_text,
        LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
              EXTRACTED_LAYOUT,
              'markdown',
              1512,
              256,
              ['\n\n', '\n', ' ', '']
           )) c;

select * from POLICY_DOCS_CHUNKS;

-- We know that all these docs are policy docs, just set categories.
-- In future, map policies or use classify_text 
update POLICY_DOCS_CHUNKS set CATEGORY = 'POLICY';

/*
-----
----- You can set categories using snowflake classify text with the below code is desirable - you may also want to map it manually to ensure it is correct.
-----

-- POC_POLICY.PROCUREMENT_POLICY.POLICY_DOCUMENTS

*/

CREATE OR REPLACE CORTEX SEARCH SERVICE POLICY_SEARCH_SERVICE 
on chunk
attributes category 
warehouse = POC
TARGET_LAG = '1 day'
as (
select 
    chunk,
    chunk_index,
    relative_path,
    file_url,
    category 
    from POLICY_DOCS_CHUNKS
);




