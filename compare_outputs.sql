
with a as (

    
  select
      publicationid as publication_id,
      doi as publication_doi,
      authorid as author_id  
  from dbt_msouare.world_monitor_legacy


),

b as (

    
  select
      publication_id,
      publication_doi,
      author_id,
  from `frontiers-playground-351416`.`dbt_msouare`.`fct_world_monitor`


),

a_intersect_b as (

    select * from a
    

    intersect distinct


    select * from b

),

a_except_b as (

    select * from a
    

    except distinct


    select * from b

),

b_except_a as (

    select * from b
    

    except distinct


    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

summary_stats as (

    select

        in_a,
        in_b,
        count(*) as count

    from all_records
    group by 1, 2

),

final as (

    select

        *,
        round(100.0 * count / sum(count) over (), 2) as percent_of_total

    from summary_stats
    order by in_a desc, in_b desc

)

select * from final


