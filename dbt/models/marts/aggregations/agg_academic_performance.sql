-- Academic Performance Aggregation by Semester
-- Pre-computed metrics for dashboard performance

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with enrollments as (
    select * from {{ ref('fact_enrollment') }}
),

students as (
    select * from {{ ref('dim_student') }}
    where is_current = true
),

semesters as (
    select * from {{ source('gold', 'dim_semester') }}
),

aggregated as (
    select
        e.semester_key,
        s.department_key,
        
        -- Student counts
        count(distinct e.student_key) as total_students,
        count(*) as total_enrollments,
        
        -- GPA metrics
        round(avg(e.gpa_points)::numeric, 2) as avg_gpa,
        round(min(e.gpa_points)::numeric, 2) as min_gpa,
        round(max(e.gpa_points)::numeric, 2) as max_gpa,
        round(stddev(e.gpa_points)::numeric, 2) as stddev_gpa,
        
        -- Pass/Fail rates
        round(
            (sum(case when e.is_passed then 1 else 0 end)::numeric / nullif(count(*), 0)) * 100,
            2
        ) as pass_rate,
        round(
            (sum(case when not e.is_passed then 1 else 0 end)::numeric / nullif(count(*), 0)) * 100,
            2
        ) as fail_rate,
        
        -- Attendance metrics
        round(avg(e.attendance_rate)::numeric, 2) as avg_attendance_rate,
        
        -- Grade distribution
        sum(case when e.grade_letter = 'A' then 1 else 0 end) as grade_a_count,
        sum(case when e.grade_letter = 'B' then 1 else 0 end) as grade_b_count,
        sum(case when e.grade_letter = 'C' then 1 else 0 end) as grade_c_count,
        sum(case when e.grade_letter = 'D' then 1 else 0 end) as grade_d_count,
        sum(case when e.grade_letter = 'F' then 1 else 0 end) as grade_f_count,
        
        current_timestamp as updated_at
        
    from enrollments e
    left join students s on e.student_key = s.student_key
    group by e.semester_key, s.department_key
)

select * from aggregated
