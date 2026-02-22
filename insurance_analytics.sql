CREATE DATABASE insurance_analytics;
USE insurance_analytics;

CREATE TABLE insurance_records (
    record_date DATE,
    year INT,
    quarter INT,
    
    age INT,
    age_group VARCHAR(30),
    sex VARCHAR(10),
    sex_female BOOLEAN,
    
    bmi DECIMAL(5,2),
    bmi_category VARCHAR(40),
    children INT,
    
    smoker VARCHAR(5),
    smoker_flag BOOLEAN,
    
    is_high_risk BOOLEAN,
    risk_score DECIMAL(4,2),
    
    region VARCHAR(15),
    region_northeast BOOLEAN,
    region_northwest BOOLEAN,
    region_southeast BOOLEAN,
    region_southwest BOOLEAN,
    
    charges DECIMAL(10,2),
    monthly_premium_est DECIMAL(10,2),
    charges_per_child DECIMAL(10,2),
    insurance_tier VARCHAR(20),
    
    bmi_age_interaction DECIMAL(10,2)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\medical_insurance_2026_kaggle.csv'
INTO TABLE insurance_records
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) as total_records FROM insurance_records;

SELECT * FROM insurance_records LIMIT 10;

SELECT MIN(year), MAX(year), COUNT(DISTINCT year) as years FROM insurance_records;
SELECT age_group, COUNT(*) as count FROM insurance_records GROUP BY age_group;
SELECT bmi_category, COUNT(*) as count FROM insurance_records GROUP BY bmi_category;
SELECT insurance_tier, COUNT(*) as count FROM insurance_records GROUP BY insurance_tier;
SELECT is_high_risk, COUNT(*) as count FROM insurance_records GROUP BY is_high_risk;

SELECT 
    record_date,
    age,
    age_group,
    sex,
    bmi,
    bmi_category,
    smoker,
    region,
    charges,
    insurance_tier
FROM insurance_records 
LIMIT 20;


-- 1. Risk Score Analysis
WITH risk_stats AS (
    SELECT 
        is_high_risk,
        COUNT(*) as count,
        ROUND(AVG(risk_score), 2) as avg_risk_score,
        ROUND(STD(risk_score), 2) as risk_stddev,
        ROUND(AVG(charges), 2) as avg_charges
    FROM insurance_records
    GROUP BY is_high_risk
)
SELECT *,
       ROUND(avg_charges / NULLIF(avg_risk_score, 0), 2) as charge_per_risk_point
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/01_risk_score_analysis.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM risk_stats;

-- 2. Risk Score Deciles
WITH risk_deciles AS (
    SELECT 
        risk_score,
        charges,
        NTILE(10) OVER (ORDER BY risk_score) as risk_decile
    FROM insurance_records
),
decile_avg AS (
    SELECT 
        risk_decile,
        ROUND(MIN(risk_score), 2) as min_risk,
        ROUND(MAX(risk_score), 2) as max_risk,
        ROUND(AVG(risk_score), 2) as avg_risk,
        ROUND(AVG(charges), 2) as avg_charges
    FROM risk_deciles
    GROUP BY risk_decile
)
SELECT *,
       ROUND(avg_charges - COALESCE(LAG(avg_charges) OVER (ORDER BY risk_decile), avg_charges), 2) as marginal_cost
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/02_risk_score_deciles.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM decile_avg
ORDER BY risk_decile;

-- 3. Feature Correlation with Charges
SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/03_feature_correlations.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM (
    WITH stats AS (
        SELECT 
            COUNT(*) as n,
            SUM(age) as sum_age,
            SUM(charges) as sum_charges,
            SUM(age * charges) as sum_age_charges,
            SUM(age * age) as sum_age_sq,
            SUM(charges * charges) as sum_charges_sq,
            SUM(bmi) as sum_bmi,
            SUM(bmi * charges) as sum_bmi_charges,
            SUM(bmi * bmi) as sum_bmi_sq,
            SUM(children) as sum_children,
            SUM(children * charges) as sum_children_charges,
            SUM(children * children) as sum_children_sq,
            SUM(risk_score) as sum_risk,
            SUM(risk_score * charges) as sum_risk_charges,
            SUM(risk_score * risk_score) as sum_risk_sq
        FROM insurance_records
    )
    SELECT 'Age' as feature,
           ROUND(
               (n * sum_age_charges - sum_age * sum_charges) / 
               SQRT((n * sum_age_sq - sum_age * sum_age) * (n * sum_charges_sq - sum_charges * sum_charges))
           , 4) as correlation_with_charges
    FROM stats
    UNION ALL
    SELECT 'BMI',
           ROUND(
               (n * sum_bmi_charges - sum_bmi * sum_charges) / 
               SQRT((n * sum_bmi_sq - sum_bmi * sum_bmi) * (n * sum_charges_sq - sum_charges * sum_charges))
           , 4)
    FROM stats
    UNION ALL
    SELECT 'Children',
           ROUND(
               (n * sum_children_charges - sum_children * sum_charges) / 
               SQRT((n * sum_children_sq - sum_children * sum_children) * (n * sum_charges_sq - sum_charges * sum_charges))
           , 4)
    FROM stats
    UNION ALL
    SELECT 'Risk Score',
           ROUND(
               (n * sum_risk_charges - sum_risk * sum_charges) / 
               SQRT((n * sum_risk_sq - sum_risk * sum_risk) * (n * sum_charges_sq - sum_charges * sum_charges))
           , 4)
    FROM stats
) as result;

-- 4. Customer Segmentation by Value and Risk
WITH customer_metrics AS (
    SELECT 
        age_group,
        insurance_tier,
        COUNT(*) as customer_count,
        ROUND(AVG(charges), 2) as avg_charges,
        ROUND(AVG(monthly_premium_est), 2) as avg_monthly,
        ROUND(AVG(risk_score), 2) as avg_risk,
        SUM(is_high_risk) as high_risk_count,
        ROUND(AVG(bmi_age_interaction), 2) as avg_health_interaction
    FROM insurance_records
    GROUP BY age_group, insurance_tier
),
segments AS (
    SELECT *,
           NTILE(4) OVER (ORDER BY avg_charges DESC) as value_quartile,
           NTILE(4) OVER (ORDER BY avg_risk DESC) as risk_quartile,
           ROUND(avg_charges * 12 * 5, 2) as estimated_5yr_ltv
    FROM customer_metrics
)
SELECT *,
       CASE 
           WHEN value_quartile = 1 AND risk_quartile = 1 THEN 'High Value - High Risk'
           WHEN value_quartile = 1 AND risk_quartile <= 2 THEN 'High Value - Low Risk'
           WHEN value_quartile >= 3 AND risk_quartile = 1 THEN 'Low Value - High Risk'
           ELSE 'Standard'
       END as customer_segment
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/04_customer_segmentation.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM segments
ORDER BY estimated_5yr_ltv DESC;

-- 5. Churn Risk Analysis
WITH churn_indicators AS (
    SELECT *,
           CASE WHEN charges > 50000 THEN 1 ELSE 0 END as is_high_cost,
           CASE WHEN risk_score > 5 THEN 1 ELSE 0 END as is_very_high_risk,
           CASE WHEN monthly_premium_est > 2000 THEN 1 ELSE 0 END as is_high_premium
    FROM insurance_records
)
SELECT 
    age_group,
    COUNT(*) as total,
    SUM(is_high_cost) as high_cost,
    SUM(is_very_high_risk) as very_high_risk,
    SUM(is_high_premium) as high_premium,
    ROUND(100.0 * (SUM(is_high_cost) + SUM(is_very_high_risk) + SUM(is_high_premium)) / (COUNT(*) * 3), 2) as churn_risk_index
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/05_churn_risk.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM churn_indicators
GROUP BY age_group
ORDER BY churn_risk_index DESC;

-- 6. Year-over-Year Trends
WITH yearly_stats AS (
    SELECT 
        year,
        COUNT(*) as policies,
        ROUND(AVG(charges), 2) as avg_charge,
        ROUND(AVG(monthly_premium_est), 2) as avg_premium,
        ROUND(AVG(risk_score), 2) as avg_risk,
        SUM(is_high_risk) as high_risk_cases
    FROM insurance_records
    GROUP BY year
)
SELECT 
    year,
    policies,
    avg_charge,
    ROUND(avg_charge - LAG(avg_charge) OVER (ORDER BY year), 2) as charge_yoy_change,
    ROUND(100.0 * (avg_charge - LAG(avg_charge) OVER (ORDER BY year)) / LAG(avg_charge) OVER (ORDER BY year), 2) as charge_yoy_pct,
    avg_risk,
    ROUND(100.0 * high_risk_cases / policies, 2) as high_risk_pct
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/06_yearly_trends.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM yearly_stats;

-- 7. Seasonal Patterns by Quarter
SELECT 
    quarter,
    COUNT(*) as total_policies,
    ROUND(AVG(charges), 2) as avg_charge,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(AVG(bmi), 2) as avg_bmi,
    SUM(smoker_flag) as smokers
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/07_seasonal_patterns.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY quarter
ORDER BY quarter;

-- 8. Regional Intelligence
WITH regional_metrics AS (
    SELECT 
        region,
        COUNT(*) as population,
        ROUND(AVG(charges), 2) as avg_charge,
        ROUND(AVG(bmi), 2) as avg_bmi,
        ROUND(AVG(risk_score), 2) as avg_risk,
        ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct,
        ROUND(100.0 * SUM(smoker_flag) / COUNT(*), 2) as smoker_pct,
        ROUND(AVG(children), 2) as avg_children
    FROM insurance_records
    GROUP BY region
)
SELECT *,
       RANK() OVER (ORDER BY avg_charge DESC) as cost_rank,
       RANK() OVER (ORDER BY high_risk_pct DESC) as risk_rank,
       RANK() OVER (ORDER BY avg_bmi DESC) as bmi_rank
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/08_regional_intelligence.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM regional_metrics
ORDER BY avg_charge DESC;

-- 9. Multivariate Risk Analysis
WITH risk_factors AS (
    SELECT 
        CASE 
            WHEN age < 30 THEN 'Young'
            WHEN age BETWEEN 30 AND 50 THEN 'Middle'
            ELSE 'Senior'
        END as age_category,
        CASE 
            WHEN bmi < 25 THEN 'Normal'
            WHEN bmi BETWEEN 25 AND 30 THEN 'Overweight'
            ELSE 'Obese'
        END as weight_category,
        smoker_flag,
        region,
        is_high_risk,
        charges,
        risk_score
    FROM insurance_records
)
SELECT 
    age_category,
    weight_category,
    smoker_flag,
    COUNT(*) as cohort_size,
    ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    ROUND(AVG(charges), 2) as avg_charges,
    ROUND(STD(charges), 2) as charge_volatility
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/09_multivariate_risk.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM risk_factors
GROUP BY age_category, weight_category, smoker_flag
ORDER BY high_risk_pct DESC, avg_charges DESC;

-- 10. Premium Pricing Analysis
WITH pricing_model AS (
    SELECT *,
           ROUND(charges / 12, 2) as actual_monthly,
           ROUND(
               500 + -- Base premium
               (age * 15) + -- Age factor
               (bmi * 20) + -- BMI factor
               (children * 250) + -- Dependent factor
               (smoker_flag * 500) + -- Smoking surcharge
               (risk_score * 300) -- Risk multiplier
           , 2) as calculated_premium
    FROM insurance_records
)
SELECT 
    insurance_tier,
    COUNT(*) as count,
    ROUND(AVG(actual_monthly), 2) as avg_actual,
    ROUND(AVG(calculated_premium), 2) as avg_calculated,
    ROUND(AVG(actual_monthly) - AVG(calculated_premium), 2) as pricing_gap,
    ROUND(100.0 * (AVG(actual_monthly) - AVG(calculated_premium)) / AVG(calculated_premium), 2) as gap_percentage
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/10_premium_pricing.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM pricing_model
GROUP BY insurance_tier
ORDER BY gap_percentage DESC;

-- 11. Outlier Detection
WITH stats AS (
    SELECT 
        AVG(charges) as avg_charges,
        STD(charges) as stddev_charges,
        AVG(risk_score) as avg_risk,
        STD(risk_score) as stddev_risk
    FROM insurance_records
)
SELECT 
    i.record_date,
    i.age,
    i.age_group,
    i.sex,
    i.bmi,
    i.bmi_category,
    i.smoker,
    i.region,
    i.charges,
    i.risk_score,
    i.insurance_tier,
    CASE 
        WHEN i.charges > (s.avg_charges + 3 * s.stddev_charges) THEN 'Extreme High Claim'
        WHEN i.charges > (s.avg_charges + 2 * s.stddev_charges) THEN 'High Claim'
        WHEN i.risk_score > (s.avg_risk + 3 * s.stddev_risk) THEN 'Extreme Risk'
        WHEN i.risk_score > (s.avg_risk + 2 * s.stddev_risk) THEN 'High Risk'
        ELSE 'Normal'
    END as anomaly_type
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/11_outlier_detection.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records i
CROSS JOIN stats s
WHERE i.charges > (s.avg_charges + 2 * s.stddev_charges)
   OR i.risk_score > (s.avg_risk + 2 * s.stddev_risk)
ORDER BY i.charges DESC
LIMIT 20;

-- 12. Cross-Sell Opportunities
SELECT 
    i.age,
    i.age_group,
    i.sex,
    i.bmi,
    i.bmi_category,
    i.children,
    i.smoker,
    i.region,
    i.charges,
    i.insurance_tier,
    i.risk_score,
    CASE 
        WHEN i.children > 2 AND i.insurance_tier = 'Bronze' THEN 'Upgrade to Family Plan'
        WHEN i.bmi > 30 AND i.smoker_flag = 0 THEN 'Wellness Program'
        WHEN i.age > 50 AND i.insurance_tier NOT IN ('Platinum', 'Diamond') THEN 'Senior Care Plan'
        WHEN i.risk_score > 4 AND i.insurance_tier = 'Bronze' THEN 'Risk Protection Upgrade'
        ELSE 'No Opportunity'
    END as cross_sell_opportunity,
    CASE 
        WHEN i.children > 2 AND i.insurance_tier = 'Bronze' THEN ROUND(i.charges * 0.3, 2)
        WHEN i.bmi > 30 AND i.smoker_flag = 0 THEN ROUND(i.charges * 0.15, 2)
        WHEN i.age > 50 AND i.insurance_tier NOT IN ('Platinum', 'Diamond') THEN ROUND(i.charges * 0.4, 2)
        WHEN i.risk_score > 4 AND i.insurance_tier = 'Bronze' THEN ROUND(i.charges * 0.25, 2)
        ELSE 0
    END as estimated_premium_increase
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/12_cross_sell.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records i
WHERE 
    (i.children > 2 AND i.insurance_tier = 'Bronze') OR
    (i.bmi > 30 AND i.smoker_flag = 0) OR
    (i.age > 50 AND i.insurance_tier NOT IN ('Platinum', 'Diamond')) OR
    (i.risk_score > 4 AND i.insurance_tier = 'Bronze')
ORDER BY estimated_premium_increase DESC;

-- 13. Cohort Analysis
WITH cohorts AS (
    SELECT *,
           CONCAT(age_group, ' - ', bmi_category) as health_cohort,
           CASE 
               WHEN year = 2021 THEN 'Cohort A (2021)'
               WHEN year = 2022 THEN 'Cohort B (2022)'
               WHEN year = 2023 THEN 'Cohort C (2023)'
               WHEN year = 2024 THEN 'Cohort D (2024)'
               ELSE 'Cohort E (2025)'
           END as signup_cohort
    FROM insurance_records
)
SELECT 
    signup_cohort,
    health_cohort,
    COUNT(*) as size,
    ROUND(AVG(charges), 2) as avg_charge,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct,
    RANK() OVER (PARTITION BY signup_cohort ORDER BY AVG(charges) DESC) as cost_rank_in_cohort
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/13_cohort_analysis.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM cohorts
GROUP BY signup_cohort, health_cohort
ORDER BY signup_cohort, cost_rank_in_cohort;

-- 14. Executive Summary
SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/14_executive_summary.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM (
    SELECT 
        'Total Premium Volume' as metric,
        CONCAT('$', FORMAT(SUM(charges), 2)) as value,
        CONCAT(FORMAT(COUNT(*), 0), ' policies') as context
    FROM insurance_records
    UNION ALL
    SELECT 'Average Premium',
        CONCAT('$', FORMAT(AVG(charges), 2)),
        CONCAT('±$', FORMAT(STD(charges), 2))
    FROM insurance_records
    UNION ALL
    SELECT 'High Risk %',
        CONCAT(FORMAT(100.0 * SUM(is_high_risk) / COUNT(*), 2), '%'),
        CONCAT(SUM(is_high_risk), ' of ', COUNT(*), ' customers')
    FROM insurance_records
    UNION ALL
    SELECT 'Smoker Rate',
        CONCAT(FORMAT(100.0 * SUM(smoker_flag) / COUNT(*), 2), '%'),
        CONCAT(SUM(smoker_flag), ' smokers')
    FROM insurance_records
    UNION ALL
    SELECT 'Avg Risk Score',
        FORMAT(AVG(risk_score), 2),
        CONCAT('Min: ', FORMAT(MIN(risk_score), 2), ', Max: ', FORMAT(MAX(risk_score), 2))
    FROM insurance_records
) as result;

-- 15. Profitability by Insurance Tier
SELECT 
    insurance_tier,
    COUNT(*) as customers,
    ROUND(SUM(charges), 2) as total_revenue,
    ROUND(AVG(charges), 2) as avg_revenue,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(100.0 * SUM(CASE WHEN is_high_risk THEN charges ELSE 0 END) / SUM(charges), 2) as high_risk_revenue_pct,
    ROUND(AVG(monthly_premium_est), 2) as avg_monthly
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/15_profitability_by_tier.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY insurance_tier
ORDER BY avg_revenue DESC;

-- 16. BMI Category Analysis
SELECT 
    bmi_category,
    COUNT(*) as count,
    ROUND(AVG(charges), 2) as avg_charges,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct,
    ROUND(100.0 * SUM(smoker_flag) / COUNT(*), 2) as smoker_pct
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/16_bmi_category.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY bmi_category
ORDER BY avg_charges DESC;

-- 17. Age Group Analysis
SELECT 
    age_group,
    COUNT(*) as count,
    ROUND(AVG(charges), 2) as avg_charges,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/17_age_group.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY age_group
ORDER BY 
    CASE age_group
        WHEN 'Young Adult (18-25)' THEN 1
        WHEN 'Adult (26-35)' THEN 2
        WHEN 'Middle-Aged (36-45)' THEN 3
        WHEN 'Senior-Middle (46-55)' THEN 4
        WHEN 'Senior (56+)' THEN 5
        ELSE 6
    END;

-- 18. Smoking Impact Analysis
SELECT 
    smoker,
    COUNT(*) as count,
    ROUND(AVG(charges), 2) as avg_charges,
    ROUND(AVG(charges) - (SELECT AVG(charges) FROM insurance_records WHERE smoker = 'no'), 2) as premium_impact,
    ROUND(AVG(risk_score), 2) as avg_risk,
    ROUND(100.0 * SUM(is_high_risk) / COUNT(*), 2) as high_risk_pct
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/18_smoking_impact.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY smoker;

-- 19. Children Impact Analysis
SELECT 
    children,
    COUNT(*) as count,
    ROUND(AVG(charges), 2) as avg_charges,
    ROUND(AVG(charges_per_child), 2) as avg_cost_per_child,
    ROUND(AVG(risk_score), 2) as avg_risk
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/19_children_impact.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records
GROUP BY children
ORDER BY children;

-- 20. Final Summary Statistics
SELECT 
    'Dataset Overview' as section,
    CONCAT(FORMAT(COUNT(*), 0), ' records') as total_records,
    CONCAT(YEAR(MIN(record_date)), '-', YEAR(MAX(record_date))) as date_range,
    CONCAT('$', FORMAT(MIN(charges), 2), ' - $', FORMAT(MAX(charges), 2)) as charges_range,
    CONCAT(FORMAT(MIN(age), 0), ' - ', FORMAT(MAX(age), 0)) as age_range
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exports/20_final_summary.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM insurance_records;
