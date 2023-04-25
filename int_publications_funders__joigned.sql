with

-- Import CTEs
aira_publications as (

        select *
        from {{ ref('stg_aira_knowledge__publications') }}
        where published_year >= 2007
),

dimensions_publications as (

        select * from {{ ref('stg_dimensions__publications') }}
        where dimensions_publication_doi is not null

),

grants as (select * from {{ ref('stg_dimensions__grants') }}),

exchange_rates as (select * from {{ ref('stg_exchange_rates__usd') }}),

-- Organization source only those where source is 24

organization_source as (

        select *
        from {{ ref('stg_aira_knowledge__organizationsources') }}
        where organizationsource_sourceid = 24

    ),

-- Logical CTEs
publications_funding as (

        select distinct
            dimensions_publication_id,
            dimensions_publication_doi,
            f.element.grid_id as publications_gridid,
            f.element.grant_id as publications_grantid
        from dimensions_publications, unnest(funding_list) as f

),

aira_publications_yearly as (

        select
            publication_doi,
            first_value(published_year) over (
                partition by publication_doi order by published_year desc
            ) as published_year
        from aira_publications

),

-- Final CTE
final as (
        -- Create a surrogate key to uniquely identify records in the final table
        select distinct
            {{
               dbt_utils.surrogate_key(
                   [
                       "publications_funding.dimensions_publication_doi",
                       "publications_funding.publications_grantid",
                       "organization_source.organizationsource_organizationid",
                    ]
               )
            }} as surrogate_key,
            publications_funding.dimensions_publication_doi,
            publications_funding.publications_grantid,
            grants.start_date,
            grants.end_date,
            grants.funding_currency as original_funding_currency,
            organization_source.organizationsource_organizationid,
            case
                grants.funding_currency
                when 'USD'
                then grants.funding_amount
                else grants.funding_amount / exchange_rates.avg_exchange_rate
            end as funding_amount_usd
        from publications_funding
        left join
            grants
            on publications_funding.publications_grantid = grants.grant_id
        left join
            aira_publications_yearly
            on publications_funding.dimensions_publication_doi
            = aira_publications_yearly.publication_doi
        left join
            organization_source
            on publications_funding.publications_gridid
            = organization_source.organizationsource_sourcevalue
        left join
            exchange_rates
            on grants.funding_currency = exchange_rates.currency_code
            and aira_publications_yearly.published_year
            = exchange_rates.exchange_rate_year

)

-- Simple Select Statement
select *
from final

