-- Fact Enrollment
-- Enrollment fact table with dimension key lookups

{{
    config(
        materialized='incremental',
        unique_key='enrollment_id',
        schema='gold',
        incremental_strategy='merge'
    )
}}

with enrollments as (
    select * from {{ ref('stg_enrollments') }}
    {% if is_incremental() %}
    where updated_at > (select max(created_at) from {{ this }})
    {% endif %}
),

students as (
    select student_key, student_id
    from {{ source('gold', 'dim_student') }}
    where is_current = true
),

courses as (
    select course_key, course_id
    from {{ source('gold', 'dim_course') }}
),

semesters as (
    select semester_key, semester_id
    from {{ source('gold', 'dim_semester') }}
),

dates as (
    select date_key, full_date
    from {{ source('gold', 'dim_date') }}
),

final as (
    select
        e.enrollment_id,
        s.student_key,
        c.course_key,
        sem.semester_key,
        d.date_key as enrollment_date_key,
        
        e.midterm_score,
        e.final_score,
        e.total_score,
        e.grade_letter,
        
        coalesce(
            e.gpa_points,
            case
                when e.total_score >= 8.5 then 4.0
                when e.total_score >= 7.0 then 3.0
                when e.total_score >= 5.5 then 2.0
                when e.total_score >= 4.0 then 1.0
                else 0.0
            end
        ) as gpa_points,
        
        e.total_score >= {{ var('passing_score') }} as is_passed,
        
        e.status,
        e.attendance_count,
        e.absence_count,
        e.attendance_rate,
        
        current_timestamp as created_at
        
    from enrollments e
    left join students s on e.student_id = s.student_id
    left join courses c on e.course_id = c.course_id
    left join semesters sem on e.semester_id = sem.semester_id
    left join dates d on e.enrollment_date = d.full_date
    
    where s.student_key is not null
      and c.course_key is not null
      and sem.semester_key is not null
)

select * from final
