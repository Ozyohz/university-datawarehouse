-- Dim Student
-- Student dimension with SCD Type 2 support

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with students as (
    select * from {{ ref('stg_students') }}
),

departments as (
    select department_key, department_id
    from {{ source('gold', 'dim_department') }}
),

enriched as (
    select
        s.student_id,
        s.full_name,
        s.date_of_birth,
        
        -- Age group calculation
        case
            when extract(year from age(current_date, s.date_of_birth)) < 20 then 'Under 20'
            when extract(year from age(current_date, s.date_of_birth)) < 25 then '20-24'
            when extract(year from age(current_date, s.date_of_birth)) < 30 then '25-29'
            when extract(year from age(current_date, s.date_of_birth)) < 35 then '30-34'
            else '35+'
        end as age_group,
        
        s.gender,
        s.email,
        s.phone,
        s.address,
        s.city,
        s.province,
        
        -- Region mapping
        case
            when s.province in ('Hà Nội', 'Hải Phòng', 'Quảng Ninh', 'Hải Dương', 'Bắc Ninh') 
                then 'Miền Bắc'
            when s.province in ('Đà Nẵng', 'Huế', 'Quảng Nam', 'Nghệ An') 
                then 'Miền Trung'
            when s.province in ('TP HCM', 'Hồ Chí Minh', 'Bình Dương', 'Đồng Nai', 'Long An') 
                then 'Miền Nam'
            else 'Khác'
        end as region,
        
        s.cohort_year,
        d.department_key,
        s.enrollment_date,
        s.graduation_date,
        
        -- Years enrolled
        case
            when s.graduation_date is not null 
                then extract(year from age(s.graduation_date, s.enrollment_date))::integer
            else extract(year from age(current_date, s.enrollment_date))::integer
        end as years_enrolled,
        
        s.status,
        
        -- SCD Type 2 columns
        current_date as effective_start_date,
        null::date as effective_end_date,
        true as is_current,
        s.created_at,
        s.updated_at
        
    from students s
    left join departments d on s.department_id = d.department_id
)

select * from enriched
