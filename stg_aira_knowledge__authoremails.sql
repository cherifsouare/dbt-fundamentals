with source as (

    select * from {{ source('aira_knowledge', 'authoremail') }}

),

transformed as (

    select
        -- ids
        authorid as authoremail_authorid,
        validityid as authoremail_validityid,

        -- dimensions
        email as authoremail_email,

        -- logical
        isprimary as is_primary,
    
        -- date/times
        verifieddate as verified_date
        
        
       
    from source

)

select * from transformed

