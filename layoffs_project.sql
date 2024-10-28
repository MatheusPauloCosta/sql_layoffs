SELECT * 
FROM layoffs;

-- Create a new table with the same values
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Remove Duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, 
 `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, 
 `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardize the Data
SELECT *
FROM layoffs_staging2;

-- Remove empty space befora and after the company name
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry = 'https://www.calcalistech.com/ctechnews/article/rysmrkfua';

SELECT *
FROM layoffs_staging2
WHERE company = 'eBay';

UPDATE layoffs_staging2
SET industry = 'Retail'
WHERE industry = 'https://www.calcalistech.com/ctechnews/article/rysmrkfua';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE location = 'DÃ¼sseldorf';

SELECT *
FROM layoffs_staging2
WHERE company = 'Springlane';

UPDATE layoffs_staging2
SET location = 'Düsseldorf'
WHERE location = 'DÃ¼sseldorf';

SELECT `date`,
STR_TO_DATE(`date`, '%Y-%m-%d')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Null values or blanc values

SELECT *
FROM layoffs_staging2
WHERE location = '' OR location IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Product Hunt';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = '' and percentage_laid_off = '' and funds_raised = '';

SELECT *
FROM layoffs_staging2
WHERE country = 'Estonia';

UPDATE layoffs_staging2
SET
	industry = NULLIF(industry, ''),
    total_laid_off = NULLIF(total_laid_off, ''),
    percentage_laid_off = NULLIF(percentage_laid_off, ''),
    stage = NULLIF(stage, ''),
    funds_raised = NULLIF(funds_raised, '')
;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Remove any columns

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Exploratory Data Analysis

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUM(total_laid_off)
FROM layoffs_staging2;

SELECT SUBSTRING(`date`, 1,7) AS `month`, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY `month`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, total_off, SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM Rolling_Total;

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), company_year_rank AS 
(SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5;

SELECT *
FROM layoffs_staging2;

SELECT company, funds_raised, total_laid_off, (funds_raised / total_laid_off) AS division
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL OR funds_raised IS NOT NULL
ORDER BY 4 DESC;


