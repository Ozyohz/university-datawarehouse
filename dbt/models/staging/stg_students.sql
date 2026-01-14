-- Staging Students
-- Selecting cleaned student data from silver layer

{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source as (
    select * from {{ source('silver', 'stg_students') }}
    where is_valid = true
),

renamed as (
    select
        student_id,
        full_name,
        date_of_birth,
        gender,
        email,
        phone,
        address,
        city,
        province,
        cohort_year,
        department_id,
        enrollment_date,
        graduation_date,
        status,
        created_at,
        updated_at
    from source
)

select * from renamed
