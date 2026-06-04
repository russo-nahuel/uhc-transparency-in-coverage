-- Overview: total rows and source files: 
-- 996 source files
-- 8868 rows
SELECT COUNT(1) as total_rows, COUNT(DISTINCT source_file) as source_files
FROM optum.uhc_tic.index_files;

-- Files with allowed_amount references
SELECT 
    COUNT(DISTINCT source_file) as files_with_allowed_amount,
    ROUND(COUNT(DISTINCT source_file) * 100.0 / (SELECT COUNT(DISTINCT source_file) FROM optum.uhc_tic.index_files), 1) as pct
FROM optum.uhc_tic.index_files
WHERE allowed_amount_location IS NOT NULL;

-- null plan_sponsor_name and affected files
SELECT COUNT(1) as null_sponsor_rows, COUNT(DISTINCT source_file) as affected_files
FROM optum.uhc_tic.index_files
WHERE plan_sponsor_name IS NULL;
--files
SELECT DISTINCT source_file
FROM optum.uhc_tic.index_files
WHERE plan_sponsor_name IS NULL;

-- Top 10 most common plans
SELECT plan_name, COUNT(*) as total
FROM optum.uhc_tic.index_files
GROUP BY plan_name
ORDER BY total DESC
LIMIT 10;

-- rows and employers per reporting entity 
-- United-HealthCare-Services-Inc alone accounts for 88.4% of all rows, serving 546 unique plans_id across 13 plan types.
    reporting_entity_name,
    COUNT(DISTINCT plan_id) as unique_plan_id,
    COUNT(DISTINCT plan_name) as unique_plans,
    COUNT(*) as total_rows,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct_of_total
FROM optum.uhc_tic.index_files
GROUP BY reporting_entity_name
ORDER BY total_rows DESC;


-- United-HealthCare-Services vs United-HealthCare-Services-Inc (naming inconsistency)
SELECT reporting_entity_name, COUNT(*) as total
FROM optum.uhc_tic.index_files
WHERE reporting_entity_name LIKE '%United-HealthCare-Services%'
GROUP BY reporting_entity_name;



-- Check if the same plan_id appears across multiple source files
SELECT 
    plan_id,
    COUNT(DISTINCT source_file) as files,
    COUNT(DISTINCT plan_sponsor_name) as sponsor_names
FROM optum.uhc_tic.index_files
GROUP BY plan_id
HAVING COUNT(DISTINCT source_file) > 1
ORDER BY files DESC
LIMIT 10;

-- Check if plan_sponsor_name and issuer_name ever differ
SELECT 
    plan_sponsor_name,
    issuer_name,
    source_file
FROM optum.uhc_tic.index_files
WHERE plan_sponsor_name != issuer_name
AND plan_sponsor_name IS NOT NULL
LIMIT 10;