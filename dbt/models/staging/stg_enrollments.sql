-- Staging Enrollments
-- Selecting cleaned enrollment data from silver layer

{{
    config(
        materialized='view',
        schema='silver'
    )
}}

with source as (
    select * from {{ source('silver', 'stg_enrollments') }}
    where is_valid = true
),

renamed as (
    select
        enrollment_id,
        student_id,
        course_id,
        semester_id,
        enrollment_date,
        midterm_score,
        final_score,
        total_score,
        grade_letter,
        gpa_points,
        status,
        attendance_count,
        absence_count,
        attendance_rate,
        created_at,
        updated_at
    from source
)

select * from renamed
