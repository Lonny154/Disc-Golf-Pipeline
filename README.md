[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=20513121&assignment_repo_type=AssignmentRepo)
# Disc Golf Weather Pipeline

## Project Overview 
Disc golf broadcasters, players, and tournament organizers frequently discuss how wind, temperature, and precipitation impact scoring — especially putting — yet these conversations are typically anecdotal.

This project builds a full data engineering pipeline to quantify the true effect of weather on putting performance at 2024 Disc Golf Major tournaments.


### Business goal
1. How does weather impact putting performance (C1X / C2) in professional disc golf?

2. Are lower-rated players affected more by poor weather than higher-rated players?

3. Can weather-adjusted insights help analysts, broadcasters, and players make better decisions?

### Analysis approach
The final analytical dataset and models allow users to:

* Understand how wind speed affects professional putting outcomes

* Compare player performance under identical weather conditions

* Surface insights like:

“Player B historically putts 78% in similar wind conditions.”

* Build dashboards & visual tools for commentators or tournament planners

* Support future reverse ETL use cases for live tournament analytics


### Data sources

#### PDGA 
Used for: 
* Event Metadata
* Round level performance
* Player level info

#### NOAA weather API
Used for: 
* Daily weather
* Station Metadata


## Design - Data engineering lifecycle details
This project aligns with the data engineering lifecycle.  The lifecycle is the foundation of the book, <u>Fundamentals of Data Engineering</u>, by Joe Ries and Matt Hously.  A screenshot of the lifecycle from the book is shown below.

![Data Engineering Lifecycle](SupplementaryInfo/DataEngineeringLifecycle.png)

### Architecture and technology choices
Lakehouse and Medallion Architecture
* Bronze - Raw PDGA and NOAA ingestion
* Siler - Cleaned, validated and normalized data
* Gold - Analytics Ready tables

Technologies Used

* Azure Databricks (primary compute and transformation layer)

* PySpark for scalable joins, unit conversions, transformations, and modeling

* Azure Data Lake / Blob Storage for multi-layer storage

* Python & Pandas for additional analysis

* Power BI for serving and visualization

### Data storage
This project uses a star schema to store Disc Golf tournament data joined with NOAA weather observations.
The central fact table (fact_player_round) records player performance per round — including scores, putting percentages, and fairway accuracy — along with linked weather metrics (temperature, wind, precipitation).

Supporting dimension tables (dim_player, dim_tournament, dim_course, dim_station) hold reference details for players, events, courses, and weather stations.
Hourly and daily NOAA data are stored in separate weather fact tables for joining by location and date.

### Ingestion
✔ PDGA Ingestion

* Pulls PDGA event list and round results for 2024 Major tournaments

* Lands raw data in Bronze storage as-is

✔ NOAA Ingestion

* Identifies nearest available NOAA stations per tournament

* Pulls daily weather observations for each event date

* Lands raw JSON/CSV into Bronze

Both ingestion flows use Databricks notebooks and Python scripts with secure credential handling.

### Transformation
Transformations include:

* Unit conversions:

  * AWND → mph

  * PRCP → mm

  * TMAX → °F

* Cleaning nulls, validating weather ranges

* Pivoting long-format weather → wide format

* Joining weather to PDGA rounds by date + city
  
* Creating analytical features:

  * Skill buckets (e.g., 950–970, 970–990, 990+)

  * Wind × skill interaction term

  * Event fixed effects

These transformations produce the Gold-layer fact table and fully denormalized serving dataset.

### Serving
Delivered Assets

* Wide analytical table containing all PDGA stats & weather variables

* Power BI dashboard for exploring:

  * Wind vs. putting performance

  * Tournament conditions

  * Player-level weather patterns

* Statistical models such as:

  * Baseline regression

  * Wind × skill interaction

  * Event fixed effects

## Undercurrents
Software Engineering

* Modularized PySpark scripts and reusable helper functions

* Version-controlled notebooks and configuration-driven processing

Data Architecture

* Lakehouse + Medallion pattern ensures scalable pipelines

* Star schema supports BI, ML, and operational analytics

Data Management

* Weather unit normalization

* Null handling and anomaly checks

* Documentation for reprocessing and reproducibility

## Implementation

### Navigating the repo

### Reproduction steps

Prerequisites

* Python 3.x

* Databricks workspace

* NOAA API token

* Access to PDGA dataset (via StatElite or local copy)

Run Order

1. Ingest PDGA data

2. Ingest NOAA weather data

3. Transform & clean (unit conversions, join logic)

4. Build Gold tables (fact + dimensions)

5. Run modeling notebook

6. Open dashboard / run analysis

Cloud Resources Required

* Azure Databricks cluster

* Azure Data Lake Storage container

* (Optional) Azure Key Vault for secrets

* Power BI Desktop or Power BI Service

