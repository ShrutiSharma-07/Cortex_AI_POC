SELECT *
FROM GPT_DOCUMENT_LINKS;

--CHECK RELO
SELECT DISTINCT A.RELATIVE_PATH, B.DOCUMENT_NAME, B.LINK
FROM POLICY_DOCS_CHUNKS A
INNER JOIN GPT_DOCUMENT_LINKS B
ON A.RELATIVE_PATH = SUBSTRING(B.DOCUMENT_NAME, POSITION('/' IN B.DOCUMENT_NAME) + 1);



--URL COLUMN RENAMED TO URL_NAME (NON WORKING LINKS)

-- Update existing table to add working links column
ALTER TABLE gpt_document_links 
ADD COLUMN LINK VARCHAR(500);

-- Update the link column based on URL(LINK) name pattern
UPDATE gpt_document_links 
SET LINK = CONCAT(
    'https://arp.nsw.gov.au/',
    LOWER(
        REPLACE(
            REPLACE(
                REPLACE(URL, ' ', '-'),
                ':', ''
            ),
            '--', '-'
        )
    )
)
WHERE URL IS NOT NULL AND DOCUMENT_NAME IS NOT NULL;


