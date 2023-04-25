with source as (

    select * from {{ source('aira_knowledge', 'authorclosestfrontiersjournal') }}

),

transformed as (

    select
        -- ids
        authorid as authorclosestfrontiersjournal_authorid,
        journalid as authorclosestfrontiersjournal_journalid,

        --measures
        confidence

    from source

)

select * from transformed