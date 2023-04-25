{% snapshot aira_knowledge__author_email_snapshots %}

{{
    config(
      
      unique_key='authorid',
      strategy='timestamp',
      updated_at='modifieddate',
    )
}}

select * from {{ source('aira_knowledge', 'authoremail') }}
where isprimary = true

{% endsnapshot %}