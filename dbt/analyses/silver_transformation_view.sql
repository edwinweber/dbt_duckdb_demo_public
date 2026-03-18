--Test hashbytes
DECLARE @HashThis NVARCHAR(32);  
SET @HashThis = CONVERT(NVARCHAR(32),'dslfdkjLK85kldhnv$n000#knf');  
SELECT HASHBYTES('SHA2_256', @HashThis); 
SELECT CHECKSUM(@HashThis);
SELECT BINARY_CHECKSUM(@HashThis);

SELECT CONCAT_WS ( ']#[', 'Edwin',ISNULL(1,''),1,ISNULL(null,''),2)

SELECT *,BINARY_CHECKSUM(CustomerType,Phone,AddressLine1) AS BinCheckSumRow,CHECKSUM(CustomerType,Phone,AddressLine1) AS CheckSumRow
FROM dbo.DimCustomer CUST

--Example of Roelant Vos PSA staging view

USE [EDW_100_Staging_Area]
GO

/****** Object:  View [dbo].[VW_STG_PROFILER_CUSTOMER_PERSONAL]    Script Date: 3/26/2015 6:39:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_STG_PROFILER_CUSTOMER_PERSONAL] AS 
WITH STG_CTE AS 
(
SELECT
   CustomerID AS CustomerID,
   Given AS Given,
   Surname AS Surname,
   Suburb AS Suburb,
   State AS State,
   Postcode AS Postcode,
   Country AS Country,
   Gender AS Gender,
   DOB AS DOB,
   Contact_Number AS Contact_Number,
   Referee_Offer_Made AS Referee_Offer_Made,
   -- Hash value to detect change
   CONVERT(CHAR(32),HASHBYTES('MD5',
      ISNULL(RTRIM(CONVERT(VARCHAR(100),CustomerID)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Given)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Surname)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Suburb)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),State)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Postcode)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Country)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Gender)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),DOB)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Contact_Number)),'NA')+'|'+
      ISNULL(RTRIM(CONVERT(VARCHAR(100),Referee_Offer_Made)),'NA')+'|'
   ),2) AS HASH_FULL_RECORD
FROM [EDW_000_Source].[dbo].[CUSTOMER_PERSONAL]
),
PSA_CTE AS
(
SELECT
   A.HASH_FULL_RECORD AS HASH_FULL_RECORD,
   A.CustomerID AS CustomerID,
   A.Given AS Given,
   A.Surname AS Surname,
   A.Suburb AS Suburb,
   A.State AS State,
   A.Postcode AS Postcode,
   A.Country AS Country,
   A.Gender AS Gender,
   A.DOB AS DOB,
   A.Contact_Number AS Contact_Number,
   A.Referee_Offer_Made AS Referee_Offer_Made
FROM EDW_150_Persistent_Staging_Area.dbo.PSA_PROFILER_CUSTOMER_PERSONAL A
   -- Join with the 'current' situation in the PSA
   JOIN (
        SELECT
            CustomerID,
            MAX(LOAD_DATETIME) AS MAX_LOAD_DATETIME
        FROM EDW_150_Persistent_Staging_Area.dbo.PSA_PROFILER_CUSTOMER_PERSONAL
        GROUP BY
         CustomerID
        ) B ON
   A.CustomerID = B.CustomerID 
   AND
   A.LOAD_DATETIME = B.MAX_LOAD_DATETIME
WHERE CDC_OPERATION != 'Delete'
)
SELECT
CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[CustomerID] ELSE STG_CTE.[CustomerID] END AS [CustomerID], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Given] ELSE STG_CTE.[Given] COLLATE DATABASE_DEFAULT END AS [Given], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Surname] ELSE STG_CTE.[Surname] COLLATE DATABASE_DEFAULT END AS [Surname], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Suburb] ELSE STG_CTE.[Suburb] COLLATE DATABASE_DEFAULT END AS [Suburb], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[State] ELSE STG_CTE.[State] COLLATE DATABASE_DEFAULT END AS [State], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Postcode] ELSE STG_CTE.[Postcode] COLLATE DATABASE_DEFAULT END AS [Postcode], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Country] ELSE STG_CTE.[Country] COLLATE DATABASE_DEFAULT END AS [Country], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Gender] ELSE STG_CTE.[Gender] COLLATE DATABASE_DEFAULT END AS [Gender], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[DOB] ELSE STG_CTE.[DOB] END AS [DOB], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Contact_Number] ELSE STG_CTE.[Contact_Number] END AS [Contact_Number], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[Referee_Offer_Made] ELSE STG_CTE.[Referee_Offer_Made] END AS [Referee_Offer_Made], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[HASH_FULL_RECORD] ELSE STG_CTE.[HASH_FULL_RECORD] COLLATE DATABASE_DEFAULT END AS [HASH_FULL_RECORD], 
  CASE WHEN STG_CTE.[CustomerID] IS NULL THEN 'Delete' WHEN PSA_CTE.[CustomerID] IS NULL THEN 'Insert' WHEN STG_CTE.CustomerID IS NOT NULL AND PSA_CTE.CustomerID IS NOT NULL AND STG_CTE.HASH_FULL_RECORD != PSA_CTE.HASH_FULL_RECORD THEN 'Change' ELSE 'No Change' END AS [CDC_OPERATION],
  'MIDAS' AS RECORD_SOURCE,
  ROW_NUMBER() OVER 
    (ORDER BY 
      CASE WHEN STG_CTE.[CustomerID] IS NULL THEN PSA_CTE.[CustomerID] ELSE STG_CTE.[CustomerID] END
    ) AS SOURCE_ROW_ID,
  GETDATE() AS EVENT_DATETIME
,CASE 
     WHEN STG_CTE.[CustomerID] IS NULL THEN 'Delete' 
     WHEN PSA_CTE.[CustomerID] IS NULL THEN 'Insert' 
     WHEN STG_CTE.CustomerID IS NOT NULL AND PSA_CTE.CustomerID IS NOT NULL AND STG_CTE.HASH_FULL_RECORD != PSA_CTE.HASH_FULL_RECORD THEN 'Change' 
     ELSE 'No Change' 
     END AS CDC_Operation
FROM STG_CTE
FULL OUTER JOIN PSA_CTE ON 
PSA_CTE.CustomerID = STG_CTE.CustomerID
WHERE 
(
  CASE 
     WHEN STG_CTE.[CustomerID] IS NULL THEN 'Delete' 
     WHEN PSA_CTE.[CustomerID] IS NULL THEN 'Insert' 
     WHEN STG_CTE.CustomerID IS NOT NULL AND PSA_CTE.CustomerID IS NOT NULL AND STG_CTE.HASH_FULL_RECORD != PSA_CTE.HASH_FULL_RECORD THEN 'Change' 
     ELSE 'No Change' 
     END 
) != 'No Change'


GO