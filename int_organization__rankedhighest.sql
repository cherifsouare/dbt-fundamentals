with

-- Import CTEs
organization as (

        select * from {{ ref('stg_aira_knowledge__organizations') }}

),

organizationtopparent as (

        select * from {{ ref('stg_aira_knowledge__organizationtopparents') }}

),

organizationranking as (

        select * from {{ ref('stg_aira_knowledge__organizationrankings') }}

),

rankings as (

        select *
        from {{ ref('stg_aira_knowledge__rankings') }}
        where ranking_id = 1

),

countries_regions as (

        select * from {{ ref('stg_frontiers_dwh__countries_regions') }}

),

-- Logical CTEs

organization_ranking as (

    select
        organizationranking.organizationranking_organizationid,
        organizationranking.organizationranking_rankingid,
        organizationranking.organizationranking_position
    from organizationranking
    left join
        rankings
        on rankings.ranking_id
        = organizationranking.organizationranking_rankingid

),

organization_countries_regions as (

    select
        organization.organization_displayname,
        organization.organization_id,
        organization.organization_countryisocode3,
        countries_regions.country,
        countries_regions.continent,
        countries_regions.regionsbin4,
        countries_regions.regionsbin7,
        countries_regions.regionsbin8,
        countries_regions.focusregionsbin,
        countries_regions.rejectionratebin
    from organization
    inner join
        countries_regions
        on countries_regions.countries_regions_countryid
        = organization.organization_countryisocode3

),

-- Final CTE

final as (
    select distinct
        organization.organization_displayname as org_name,
        organization.organization_id,
        parent_organisation.organization_displayname as parent_org_name,
        organization.organization_countryisocode3,
        organization_countries_regions.country,
        organization_countries_regions.continent,
        organization_countries_regions.regionsbin4,
        organization_countries_regions.regionsbin7,
        organization_countries_regions.regionsbin8,
        organization_countries_regions.focusregionsbin,
        organization_countries_regions.rejectionratebin,
        organization_ranking.organizationranking_position as org_rank,
        row_number() over (
            partition by organization.organization_id
            order by organization_ranking.organizationranking_rankingid
        ) as highest_ranked
    from organization
    left join
        organizationtopparent
        on organizationtopparent.organizationtopparent_id
        = organization.organization_id
    left join
        organization as parent_organisation
        on organizationtopparent.topparentorganization_id
        = parent_organisation.organization_id
    left join
        organization_ranking
        on organizationtopparent.topparentorganization_id
        = organization_ranking.organizationranking_organizationid
    left join
        organization_countries_regions
        on organizationtopparent.topparentorganization_id
        = organization_countries_regions.organization_id
    qualify highest_ranked = 1

)

-- Simple Select statement
select * from final

















