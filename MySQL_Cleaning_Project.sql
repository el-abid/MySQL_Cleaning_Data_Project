SELECT * 
FROM layoffs;




-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT distinct *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remouve duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remouve Any Column


-- 1. Remove Duplicates

SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off,country, funds_raised_millions, `date`
) AS ROW_NUM
FROM layoffs_staging;



WITH DUPLICATES_CTE AS 
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions
) AS ROW_NUM
FROM layoffs_staging
)

SELECT * 
FROM DUPLICATES_CTE
WHERE ROW_NUM > 1;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `ROW_NUM` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
WHERE ROW_NUM > 1;

INSERT INTO layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off,country, funds_raised_millions, `date`
) AS ROW_NUM
FROM layoffs_staging;

delete from layoffs_staging2 WHERE ROW_NUM > 1;


SELECT * 
FROM layoffs_staging2;


-- STANDERDIZING DATA
-- deleting spaces using trim() fonction

SELECT trim(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = trim(company);

SELECT company, trim(company)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

-- LET'S TAKE A LOOK AT INDUSTRY IF WE CAN DO ANY CHANGES

SELECT DISTINCT INDUSTRY
FROM layoffs_staging2
ORDER BY INDUSTRY;

-- IT LOOKS WE HAVE SOME OF NULL COLUMNS AND SOME OF IT ARE EMPTY 
-- AND WE HAVE CRYPTO AND CRYPTOCURRENCY INDUSTRY REFERS TO SAME ACTIVITY BUT THEY ARE DEFINED DIFFRENT
-- LETS TRY TO FIX THAT 

-- FIRST LETS TRY TO FIX NULLS AND SEE IF WE CAN FILL THEM 
-- TO DO THAT WE SEE THE OTHER SIMILAR ROWS AND DO COPY/PAST

SELECT * 
FROM layoffs_staging2
WHERE INDUSTRY IS NULL
OR INDUSTRY = ''
ORDER BY INDUSTRY;


SELECT *
FROM layoffs_staging2
WHERE COMPANY LIKE 'BALLY%';
-- IT LOOKS WE CAN'T FIX THESE ONES CAUSE OF LACK OF INDUSSTRY SIMILAR ROWS

-- lets try with Airbnb
SELECT *
FROM layoffs_staging2
WHERE COMPANY LIKE 'airbnb%';

-- As we can see from the other Airbnb column company is a Travel industry we can fill the empty airbnb columns with Travel

-- first we need to turn empty column to NULL

 update layoffs_staging2
 set industry = null
 where industry = '';
 
 -- Then we use allias to create a copy of the table in order to update colomns we need 
 
 select t1.industry, t1.company,  t2.industry, t2.company
 from layoffs_staging2 t1
 join layoffs_staging2 t2
	ON t1.company = t2.company
    where (t1.industry is null or t1.industry='')
    and t2.industry is not null;
 
 UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
where (t1.industry is null or t1.industry='')
    and t2.industry is not null;
 
  select company, industry
 from layoffs_staging2
 where company = 'Airbnb';
 
 -- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- LETS MOVE AND TRY TO FIX CRYPTO ISSUE

SELECT * 
FROM layoffs_staging2
WHERE INDUSTRY LIKE 'CRYPTO%';

SELECT INDUSTRY 
FROM layoffs_staging2
WHERE INDUSTRY LIKE 'CRYPTO_%';

update layoffs_staging2
set industry = 'Crypto'
WHERE INDUSTRY LIKE 'crypto';
-- lets check 

SELECT industry
FROM layoffs_staging2
order by industry;

-- all right we also need to look at the other rows like country

SELECT *
FROM layoffs_staging2;

SELECT distinct country
FROM layoffs_staging2;

-- we have an issue where United States and United States. are defined different 

update layoffs_staging2
set country = 'United States'
where country like 'United States%';

-- now if we run this again it is fixed

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- Let's also fix the date columns
SELECT *
FROM layoffs_staging2;


update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

-- Drop useless rows
-- in our case is ROW_NUM we have created eirlier

alter table layoffs_staging2
drop column ROW_NUM;

SELECT *
FROM layoffs_staging2;

-- end




