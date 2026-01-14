-- Cohort Retention Analysis
-- Analyzes student retention and graduation rates by cohort

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with students as (
    select * from {{ ref('dim_student') }}
    where is_current = true
),

cohort_summary as (
    select
        cohort_year,
        department_key,
        
        -- Cohort sizes
        count(*) as initial_students,
        sum(case when status = 'Active' then 1 else 0 end) as current_students,
        sum(case when status = 'Graduated' then 1 else 0 end) as graduated_students,
        sum(case when status in ('Dropped', 'Suspended') then 1 else 0 end) as dropped_students,
        
        -- Rates
        round(
            (sum(case when status = 'Active' then 1 else 0 end)::numeric / nullif(count(*), 0)) * 100,
            2
        ) as retention_rate,
        
        round(
            (sum(case when status = 'Graduated' then 1 else 0 end)::numeric / nullif(count(*), 0)) * 100,
            2
        ) as graduation_rate,
        
        -- Time to graduate (for graduated students)
        round(
            avg(
                case 
                    when status = 'Graduated' and graduation_date is not null 
                    then years_enrolled 
                end
            )::numeric,
            1
        ) as avg_years_to_graduate,
        
        -- Academic performance
        round(avg(
            case 
                when status = 'Graduated' 
                then 3.5  -- This should come from actual cumulative GPA
            end
        )::numeric, 2) as avg_graduate_gpa,
        
        current_timestamp as updated_at
        
    from students
    where cohort_year is not null
    group by cohort_year, department_key
)

select * from cohort_summary
order by cohort_year desc, department_key
