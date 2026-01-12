/*
Project: SQL Data Cleaning – Global Layoffs Dataset
Source: Kaggle
https://www.kaggle.com/datasets/swaptr/layoffs-2022
Objective:
- Clean raw layoff data
- Remove duplicates and invalid records
- Standardize fields for analysis

Outcome:
Cleaned dataset ready for EDA and reporting
*/


SELECT *
FROM global_layoffs.layoffs;

-- Creating a staging table to preserve raw data
-- This allows rollback and auditability during cleaning
CREATE TABLE global_layoffs.layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * 
FROM global_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates


-- Identifying duplicate records based on business-relevant fields
-- These columns uniquely represent a layoff event
-- ROW_NUMBER is used to retain the earliest occurrence

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,stage,funds_raised_millions,country) AS row_num
FROM layoffs_staging
)
SELECT * FROM
duplicate_cte 
WHERE row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially

-- creating layoffs_staging2 same as layoffs_staging1 with an extra 'row_num' column to help delete duplicate rows (keep only 1).

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,stage,funds_raised_millions,country) AS row_num
FROM layoffs_staging;

-- Verifying duplicate count before deletion
SELECT COUNT(*) AS duplicate_rows
FROM layoffs_staging2
WHERE row_num > 1;

-- now that we have this we can delete rows where row_num > 1 (keeping one valid record)

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Confirming duplicates are removed
SELECT COUNT(*) AS remaining_duplicates
FROM layoffs_staging2
WHERE row_num > 1;



-- 2. Standardizing data


-- removing leading and trailing spaces from company names to ensure consistency.

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM global_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM global_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- this query is fine; no issues here.
SELECT *
FROM global_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- this query is fine; no issues here.

SELECT *
FROM global_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE global_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM global_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM global_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- Crypto has multiple different variations. We need to standardize that - let's set all to Crypto
SELECT DISTINCT industry
FROM global_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM global_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM global_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM global_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM global_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM global_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM global_layoffs_layoffs.layoffs_staging2;





-- 3. Look at Null Values

-- NULLs in total_laid_off, percentage_laid_off, and funds_raised_millions seem appropriate.
-- keeping them as NULL is useful for accurate calculations during the EDA phase.

-- no changes needed for NULL values at this point.


-- 4. remove any columns and rows we need to

SELECT *
FROM global_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM global_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM global_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM global_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final cleaned dataset ready for analysis
-- All duplicates removed, formats standardized, and invalid records excluded
SELECT * 
FROM global_layoffs.layoffs_staging2;


-- High-level dataset summary
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT company) AS unique_companies
FROM layoffs_staging2;