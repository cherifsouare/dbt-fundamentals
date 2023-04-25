
# World Monitor -Frontiers Ocean

## Overview

This project builds and maintains the [dbt](https://www.getdbt.com/product/dbt-cloud-enterprise/) models generated from 
processing raw data stored in Fontiers Ocean. 
The underlying data includes external sources which comprise of AIRA-Knowledge (the main source),[dimensions](https://www.dimensions.ai/),
Web of Science and internal data from Frontiers Data Warehouse.

The final outputs from this transformation process are used to power the
[World Monitor- Frontiers Ocean](https://webportal.dwh.frontiersin.net/#/workbooks/975/views) Tableau dashboard with a scope
to explore and provide insights into the global publishing markets.


## Structure
From a design perspective perspective, the project follows [dbt projects structure best practices](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview) and is structured as follows.
```
dbt-world-monitor/
├─ .github/
├─ analyses/
├─ dbt_packages/
├─ logs/
├─ macros/
├─ models/
│  ├─ marts
│  ├─ intermediate
│  ├─ staging
├─ seeds/
├─ snapshots/
├─ target/
├─ tests/
├─ .gitignore
├─ package.json
├─ README.md

```
## Documentation

Description of all sources, models and their downstream use are configured to be auto_generated 
and accessible to data consumers on [the project documentation site](https://dbt-world-monitor-dot-frontiers-infraestructure.ey.r.appspot.com/#!/overview)**
(Use Frontiers Gmail Account to Login)

## Screenshots

![WM-DAG](https://user-images.githubusercontent.com/114750927/206677274-cb682015-50cb-4fb7-b8d9-57ce86bef759.png)

## Deployment

Deployment is implemented using a 1 trunk / direct promotion approach with the following steps.

 #### 1. Development
 ###### Run locally by the developer in **[Fontiers Exports Sandbox](https://console.cloud.google.com/bigquery?project=frontiers-exports-sandbox&ws=!1m0)**

```
  Preview
  dbt build --select package:dbt_project_evaluator
  dbt build
```
#### 2. Continuous Integration (Slim CI)
######  Run on pull requests typically in **[Fontiers Exports Sandbox](https://console.cloud.google.com/bigquery?project=frontiers-exports-sandbox&ws=!1m0)**

```
dbt build --select state:modified+ --exclude package:dbt_project_evaluator
dbt build --select package:dbt_project_evaluator
```

#### 3. Production
###### Regular Job scheduled to run weekly or on demand in **[Fontiers Ocean Exports](https://console.cloud.google.com/bigquery?project=frontiers-ocean-exports&ws=!1m0)

```
dbt  test -s source:* --exclude package:dbt_project_evaluator
dbt  run --exclude package:dbt_project_evaluator
dbt test --exclude source:* --exclude package:dbt_project_evaluator
dbt docs generate --exclude package:dbt_project_evaluator
```

## Contributing

More on this later.

## Resources for dbt:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
