<h1 align="center" style="color:#0B2D5C; font-size: 40px; margin-bottom: 8px;">
  𝙇𝙤𝙤𝙠𝙚𝙧 𝙎𝙩𝙪𝙙𝙞𝙤 𝘿𝙖𝙨𝙝𝙗𝙤𝙖𝙧𝙙
</h1>

## <span style="color:#0B2D5C;">**𝙊𝙫𝙚𝙧𝙫𝙞𝙚𝙬**</span>
This document presents the final visualization layer of the **Ecodata - Cloud** project. 
The dashboard is built in Looker Studio and connects directly to our BigQuery data warehouse, specifically the `ecodatacloud_bq_gold.gold__obt` table.

Because the One Big Table (OBT) is pre-joined, partitioned by `year`, and clustered by `country_label`, the dashboard loads quickly and allows for seamless cross-country comparisons.

---

## <span style="color:#0B2D5C;">**𝘿𝙖𝙨𝙝𝙗𝙤𝙖𝙧𝙙 𝙇𝙞𝙣𝙠**</span>
> 🔗 **Click here to access the live Looker Studio Report** *(Replace this with your actual Looker Studio public/shared link)*

---

## <span style="color:#0B2D5C;">**𝙆𝙚𝙮 𝙑𝙞𝙨𝙪𝙖𝙡𝙞𝙯𝙖𝙩𝙞𝙤𝙣𝙨**</span>

### 1. Global Macroeconomic Overview
This section provides a high-level view of the world economy for a selected year. It answers questions like:
- *Which economies generate the most wealth?*
- *Where is unemployment highest?*

<p align="center">
  <img src="./ressources/pictures/looker_overview_placeholder.png" alt="Dashboard Overview" width="800" />
  <br>
  <i>*(Drop your overview screenshot in the ressources folder and update the path above)*</i>
</p>

### 2. Country Comparison (Debt vs. Wealth)
This scatter plot or bar chart compares General Government Gross Debt (`public_debt_gdp_percent_gdp`) against GDP per capita PPP (`gdp_per_capita_ppp_international_dollars_per_capita`). It helps identify heavily indebted nations versus high-income nations.

<p align="center">
  <img src="./ressources/pictures/looker_comparison_placeholder.png" alt="Country Comparison" width="800" />
</p>

### 3. Historical Trends (Inflation & Unemployment)
Using the `year` partition, this time-series chart tracks the evolution of inflation (`inflation_avg_consumer_percent_change`) and unemployment (`unemployment_rate_percent_labor_force`) over the decades for selected major economies.

<p align="center">
  <img src="./ressources/pictures/looker_trends_placeholder.png" alt="Historical Trends" width="800" />
</p>

---

## <span style="color:#0B2D5C;">**𝙐𝙨𝙖𝙜𝙚 𝙄𝙣𝙨𝙩𝙧𝙪𝙘𝙩𝙞𝙤𝙣𝙨**</span>
- **Filters**: Use the global filters at the top of the dashboard to select specific `Years` or isolate specific `Country Labels`.
- **Interactivity**: Charts are cross-filtered. Clicking on a specific country in a bar chart will filter the historical trend lines for that exact country.