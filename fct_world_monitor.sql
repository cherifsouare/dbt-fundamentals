with
-- Import CTEs
publications as (
    
    select * from {{ ref('int_publications__joigned') }}
    
),

authors as (
    
    select * from {{ ref('int_authors__joigned') }}


),

organizations as (
    
    
    select * from {{ ref('int_organization__rankedhighest') }}
    
),

publications_funders as (
        
        select * from {{ ref('int_publications_funders__joigned') }}
        
),

grants as (
        
        select * from {{ ref('int_grants_only__joigned') }}
        
    
),

-- Logical CTEs

all_publications_funders as (
    select distinct
        publications_funders.dimensions_publication_doi,
        publications_funders.publications_grantid,
        publications_funders.start_date,
        publications_funders.end_date,
        publications_funders.funding_amount_usd,
        organizations.organization_id,
        organizations.parent_org_name as funder_name,
        organizations.country as funder_country,
        organizations.regionsbin4 as funder_regionsbins4,
        organizations.regionsbin7 as funder_regionsbins7,
        organizations.regionsbin8 as funder_regionsbins8,
        organizations.focusregionsbin as funder_focusregionsbin
    from publications_funders
    left join
        organizations
        on publications_funders.organizationsource_organizationid
        = organizations.organization_id

),

publications_joigned as (
    select
        distinct publications.publication_id,
        publications.publication_doi,
        publications.published_year,
        publications.published_date,
        publications.publication_type,
        publications.journal_id,
        publications.journal_name,
        publications.journal_impact_factor,
        case
            when publications.jifpercentile is null
            then 'n/a'
            when publications.jifpercentile <= 10
            then '0-10'
            when publications.jifpercentile <= 20
            then '10-20'
            when publications.jifpercentile <= 30
            then '20-30'
            when publications.jifpercentile <= 40
            then '30-40'
            when publications.jifpercentile <= 50
            then '40-50'
            when publications.jifpercentile <= 60
            then '50-60'
            when publications.jifpercentile <= 70
            then '60-70'
            when publications.jifpercentile <= 80
            then '70-80'
            when publications.jifpercentile <= 90
            then '80-90'
            when publications.jifpercentile <= 100
            then '90-100'
        end as jif_percentile_bin,
        publications.is_openaccess,
        publications.open_access_category,
        publications.publisher_name,
        publications.cites,
        authors.authorfulllist_authorid as author_id,
        authors.full_name,
        authors.primary_email_address,
        case
            when authors.authorsource_authorid is null then 'no' else 'yes'
        end as is_ebmauthor,
        case
            when authors.contacted_campaign_email is null then 'no' else 'yes'
        end as has_authorbeencontacted_viacampaign,
        {{ authors_from_publishers('authors.frontiers_pubs') }} as is_frontiers_author,
        {{ authors_from_publishers('authors.mdpi_pubs') }} as is_mdpi_author,
        {{ authors_from_publishers('authors.elsevier_pubs') }} as is_elsevier_author,
        {{ authors_from_publishers('authors.springer_nature_pubs') }} as is_springernature_author,
        {{ authors_from_publishers('authors.wiley_pubs') }} as is_wiley_author,
        {{ authors_from_publishers('authors.ieee_pubs') }} as is_ieee_author,
        {{ authors_from_publishers('authors.taylor_francis_pubs') }} as is_taylorandfrancis_author,
        case
            when authors.all_pubs = 1
            then '1'
            when authors.all_pubs > 1 and authors.all_pubs <= 3
            then '2-3 articles published'
            when authors.all_pubs > 3 and authors.all_pubs <= 5
            then '4-5 articles published'
            when authors.all_pubs > 5 and authors.all_pubs <= 10
            then '6-10 articles published'
            when authors.all_pubs > 10 and authors.all_pubs <= 20
            then '11-20 articles published'
            when authors.all_pubs > 20 and authors.all_pubs <= 30
            then '21-30 articles published'
            when authors.all_pubs > 30 and authors.all_pubs <= 50
            then '31-50 articles published'
            when authors.all_pubs > 50 and authors.all_pubs <= 100
            then '51-100 articles published'
            else '+100'
        end as articles_published_bins,
        authors.frontiers_closest_journal,
        authors.confidence as frontiers_closest_journalconfidence,
        authors.author_field_of_study,
        authors.author_field_of_study_l1,
        authors.author_field_of_study_l2,
        authors.has_validemail,
        authors.user_highest_role_id,
        authors.user_highest_role,
        authors.author_hindex as hindex,
        case
            when authors.author_hindex is null
            then 'no h-index'
            when authors.author_hindex = 0
            then '0'
            when authors.author_hindex < 5
            then '1-4'
            when authors.author_hindex < 10
            then '5-9'
            when authors.author_hindex < 20
            then '10-19'
            when author_hindex < 30
            then '20-29'
            else '30+'
        end as h_index_bins,
        authors.influence,
        {{ scienceradar_bins('authors.influence') }} as influence_bins,
        authors.connectivity,
        {{ scienceradar_bins('authors.connectivity') }} as connectivity_bins,
        authors.activeness,
        {{ scienceradar_bins('authors.activeness') }} as activity_bins,
        authors.productivity,
        {{ scienceradar_bins('authors.productivity') }} as productivity_bins,
        authors.trendiness,
        {{ scienceradar_bins('authors.trendiness') }} as trendiness_bins,
        organizations.org_name,
        organizations.parent_org_name,
        organizations.org_rank,
        case
            when organizations.org_name is null and org_rank is null
            then 'n/a'
            when organizations.org_rank is null
            then 'non-ranked'
            when organizations.org_rank <= 25
            then 'top 25'
            when organizations.org_rank <= 150
            then 'top 150'
            when organizations.org_rank <= 300
            then 'top 300'
            when organizations.org_rank <= 500
            then 'top 500'
            when organizations.org_rank <= 1000
            then 'top 1000'
            else 'top 1000+'
        end as org_rank_bins,
        if
        (organizations.org_rank <= 150, 'yes', 'no') as org_top150,
        organizations.organization_countryisocode3 as countryisocode3,
        organizations.country,
        organizations.continent,
        organizations.regionsbin4,
        organizations.regionsbin7,
        organizations.regionsbin8,
        organizations.focusregionsbin,
        organizations.rejectionratebin,
        all_publications_funders.publications_grantid as grant_id,
        all_publications_funders.start_date as grant_start_date,
        all_publications_funders.end_date as grant_end_date,
        all_publications_funders.funder_name,
        all_publications_funders.funder_country,
        all_publications_funders.funder_regionsbins4,
        all_publications_funders.funder_regionsbins7,
        all_publications_funders.funder_regionsbins8,
        all_publications_funders.funder_focusregionsbin,
        all_publications_funders.funding_amount_usd,
    from publications
    left join
        authors
        on publications.publication_id
        = authors.publicationauthor_publicationid
    left join 
        organizations
        on organizations.organization_id
        = authors.author_lastknownaffiliationid
    left join 
        all_publications_funders
        on all_publications_funders.dimensions_publication_doi
        = publications.publication_doi

),

joigned_with_author_number as (
    select
        *,
        row_number() over (
            partition by
                publications_joigned.publication_id,
                publications_joigned.author_id
        ) as author_number
    from publications_joigned
),


final as (

    select

        -- Create a surrogate key to identify unique records
        {{ dbt_utils.surrogate_key(["publication_id", "author_id", "author_number"]) }}
        as surrogate_key,

        --Articles
        publication_id,
        publication_doi,
        published_year, 
        published_date,
        publication_type,
        journal_id,
        journal_name,
        journal_impact_factor,
        jif_percentile_bin,
        is_openaccess,
        open_access_category,
        publisher_name,
        cites,

        --Authors
        author_id,
        full_name,
        primary_email_address,
        is_ebmauthor,
        has_authorbeencontacted_viacampaign,
        is_frontiers_author,
        is_mdpi_author,
        is_elsevier_author,
        is_springernature_author,
        is_wiley_author,
        is_ieee_author,
        is_taylorandfrancis_author,
        articles_published_bins,
        frontiers_closest_journal,
        frontiers_closest_journalconfidence,
        author_field_of_study,
        author_field_of_study_l1,
        author_field_of_study_l2,
        has_validemail,
        user_highest_role_id,
        user_highest_role,

        -- Science Radar Metrics
        hindex,
        h_index_bins,
        influence,
        influence_bins,
        connectivity,
        connectivity_bins,
        activeness,
        activity_bins,
        productivity,
        productivity_bins,
        trendiness,
        trendiness_bins,

        --Organization
        org_name,
        parent_org_name,
        org_rank,
        org_rank_bins,
        org_top150,

        --Geolocation
        countryisocode3,
        country,
        continent,
        regionsbin4,
        regionsbin8,
        focusregionsbin,
        rejectionratebin,

        --Grants
        grant_id,
        grant_start_date,
        grant_end_date,
        funder_name,
        funder_country,
        funder_regionsbins4,
        funder_regionsbins7,
        funder_regionsbins8,
        funder_focusregionsbin,
        funding_amount_usd,

        --Additionnal Info
        1 / count(*) over (
            partition by publication_id, author_id
        ) as author_weight,
        (
            1 / count(*) over (partition by publication_id, author_id)
        ) / count(distinct author_id) over (
            partition by publication_id
        ) as weight,
        'articles' as table_source
    from joigned_with_author_number

       
       union all


    select
        
        --Add surrogate key
        null as surrogate_key,
        
        --Articles
        null as publication_id,
        ''  as publication_doi,
        grants.start_year as published_year,
        cast(null as date) as published_date,
        '' as publication_type,
        null as journal_id,
        '' as journal_name,
        '' as journal_impact_factor,
        '' as jif_percentile_bin,
        cast(null as bool) as is_openaccess,
        '' as open_access_category,
        '' as publisher_name,
        null as cites,

        --Authors
        null as author_id,
        '' as full_name,
        '' as primary_email_address,
        '' as is_ebm_author,
        '' as has_authorbeencontacted_viacampaign,
        '' as is_frontiers_author,
        '' as is_mdpi_author,
        '' as is_elsevier_author,
        '' as is_springernature_author,
        '' as is_wiley_author,
        '' as is_ieee_author,
        '' as is_taylorandfrancis_author,
        '' as articles_published_bins,
        '' as frontiers_closest_journal,
        null as frontiers_closest_journalconfidence,
        '' as author_field_of_study,
        '' as author_field_of_study_l1,
        '' as author_field_of_study_l2,
        '' as has_validemail,
        '' as user_highest_role_id,
        '' as user_highest_role,

        -- Science Radar Metrics
        null as hindex,
        '' as h_index_bins,
        null as influence,
        '' as influence_bins,
        null as connectivity,
        '' as connectivity_bins,
        null as activeness,
        '' as activity_bins,
        null as productivity,
        '' as productivity_bins,
        null as trendiness,
        '' as trendiness_bins,

        --Organization
        '' as org_name,
        '' as parent_org_name,
        null as org_rank,
        '' as org_rank_bins,
        '' as org_top150,

        --Geolocation
        '' as countryisocode3,
        '' as country,
        '' as continent,
        '' as regionsbin4,
        '' as regionsbin8,
        '' as focusregionsbin,
        '' as rejectionratebin,

        --Grants
        grants.grant_id,
        grants.start_date as grant_start_date,
        grants.end_date as grant_end_date,
        organizations.parent_org_name as funder_name,
        organizations.country as funder_country,
        organizations.regionsbin4 as funder_regionsbins4,
        organizations.regionsbin7 as funder_regionsbins7,
        organizations.regionsbin8 as funder_regionsbins8,
        organizations.regionsbin8 as funder_focusregionsbin,
        grants.funding_amount_usd,
        
        --Additional Info
        null as author_weight,
        null as weight,
        'grants' as table_source
    from grants
    left join
        organizations
        on grants.organizationsource_organizationid
        = organizations.organization_id
)


-- Simple select statement

select * from final







