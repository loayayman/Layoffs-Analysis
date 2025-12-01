-- Active: 1758446815945@@127.0.0.1@3306@layoffs_projcet
SELECT * FROM layoffs1;

-- Creating a backup ROW data 
CREATE TABLE layoffs_row
LIKE layoffs1;

INSERT INTO layoffs_row
SELECT * FROM layoffs1;

SELECT * FROM layoffs_row;


-- Data Cleaning 

-- 1.Remove Duplicates 
-- IDENTIFYING THE DUPLICATES
WITH duplicate_cte AS
(
SELECT * , ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS Row_num
FROM layoffs1
)
SELECT * FROM duplicate_cte 
WHERE Row_num >1;

-- CREATE A DUPLICATES-FREE TABLE
CREATE TABLE `layoffs` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs;

INSERT INTO layoffs
SELECT * , ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS Row_num
FROM layoffs1;

SELECT * FROM layoffs
WHERE row_num >1;

DELETE FROM layoffs
WHERE row_num >1;

-- Standardizing data

SELECT company,TRIM(company)
FROM layoffs;

UPDATE layoffs 
SET company = TRIM(company);

SELECT  DISTINCT industry
FROM layoffs
ORDER BY 1;

SELECT  *
FROM layoffs
WHERE industry LIKE 'Crypto%';

UPDATE layoffs
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location 
FROM layoffs 
ORDER BY 1;

SELECT DISTINCT country 
FROM layoffs 
ORDER BY 1;

SELECT DISTINCT country, TRIM(country)
FROM layoffs 
WHERE country LIKE 'United States%';

UPDATE layoffs 
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT `date`,STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs;

UPDATE layoffs
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs
ORDER BY 1;

ALTER TABLE layoffs
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs;


-- NULLS AND BLANK VALUES

SELECT * FROM layoffs
WHERE industry IS NULL 
OR industry = '';

UPDATE layoffs
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoffs
WHERE company= 'Airbnb';

SELECT t1.location, t1.company, t1.industry, t2.location ,t2.company, t2.industry
FROM layoffs t1 
JOIN layoffs t2 
	ON t1.company = t2.company 
	AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs t1 
JOIN layoffs t2 
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs;

SELECT * FROM layoffs
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- WE CAN'T FILL OUT THESE NULL ROWS 

DELETE FROM layoffs 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs;

-- WE DON'T NEED row_num COLUMN ANYMORE 
ALTER TABLE layoffs
DROP COLUMN row_num;

SELECT * FROM layoffs;
SELECT COUNT(*) FROM layoffs;
-- WE'RE DONE WITH CLEANING 


-- EXPLORATORY DATA ANALYSIS
-- EDA

SELECT * FROM layoffs;

SELECT MAX(total_laid_off) , MAX(percentage_laid_off)
FROM layoffs;

SELECT * FROM layoffs
WHERE percentage_laid_off = 1;

SELECT COUNT(*) FROM layoffs
WHERE percentage_laid_off = 1; -- WE GOT 116 COMPANIES HAS 100% LAID OFF PERCENTAGE 

SELECT * FROM layoffs
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; 

SELECT MIN(`date`) AS Min_date, MAX(`date`) AS Max_date
FROM layoffs;

SELECT company, 
SUM(total_laid_off)
FROM layoffs
GROUP BY company
ORDER BY 2 DESC; -- THE HIGHEST LAID OFF COMPANIES THROUGH OUT THIS THREE YEARS DATA

SELECT industry ,MAX(total_laid_off) 
FROM layoffs
GROUP BY industry
ORDER BY 2 DESC; -- THE HIGHEST LAID OFF INDUSTRIES

SELECT country ,SUM(total_laid_off)
FROM layoffs
GROUP BY country 
ORDER BY 2 DESC; -- TOP LAID OFF COUNTRIES

SELECT YEAR(date) ,SUM(total_laid_off)
FROM layoffs
GROUP BY YEAR(date); -- TOP YEARS IN TERM OF TOTAL LAID OFFS

SELECT stage, SUM(total_laid_off)
FROM layoffs
GROUP BY stage
ORDER BY 2 DESC;

SELECT MONTH(`date`) AS `Month`, SUM(total_laid_off) 
FROM layoffs
GROUP BY MONTH(`date`)
ORDER BY SUM(total_laid_off) DESC; -- TOP MONTHS

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
, SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_total
FROM Rolling_total;

SELECT company, YEAR(`date`),
SUM(total_laid_off)
FROM layoffs
GROUP BY company, YEAR(`date`) 
ORDER BY 3 DESC;


WITH company_year (company, years, total_laid_off)AS 
(
SELECT company, YEAR(`date`),
SUM(total_laid_off)
FROM layoffs
GROUP BY company, YEAR(`date`) 
), 
Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5; -- TOP 5 LAID OFF COMPINES EVERY YEAR



WITH Industry_year (industry, years, total_laid_off)AS 
(
SELECT industry, YEAR(`date`),
SUM(total_laid_off)
FROM layoffs
GROUP BY Industry, YEAR(`date`) 
), 
Industry_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Industry_year
WHERE years IS NOT NULL)
SELECT * 
FROM Industry_Year_Rank
WHERE Ranking <= 5; -- TOP 5 LAID OFF INDUSTRIES EVERY YEAR