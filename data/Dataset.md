<p align="center"><img src="../ressources/pictures/fmi_logo.jpg" alt="fmi_logo" width="180"></p>

### Purpose
This page provides context for the dataset used in this project and explains the key IMF concepts, the indicators selected, and how to retrieve the data via the IMF DataMapper API.

### Data source (main dataset)
- **IMF DataMapper — World Economic Outlook (WEO) dataset**: https://www.imf.org/external/datamapper/datasets/WEO
- Project questions this dataset can help answer:
  - Which economies generate the most wealth?
  - Which are the most indebted?
  - Where is unemployment highest?

### About the IMF (International Monetary Fund)
The **International Monetary Fund (IMF)** is an international organization that works to support **global monetary cooperation** and **financial stability**. It produces widely used macroeconomic statistics and forecasts, including the **World Economic Outlook (WEO)**.

In practice, IMF datasets are useful because they provide:
- **Standardized definitions** across countries (as much as possible)
- **Comparable units** (often in percent of GDP, per-capita values, or international dollars)
- **Time series** suitable for trend analysis and cross-country comparisons

### Dataset concepts (how to read the indicators)
The indicators below are **macro indicators** unless explicitly stated otherwise. “Macro” means the values describe an **economy or country as a whole**, not individual households.

> **Interpretation tips**
> - Always check the **unit** (percent, USD, percent of GDP).
> - When comparing living standards across countries, prefer **PPP-based** indicators.
> - Be careful with rankings across years: changes can come from **inflation**, **exchange rates**, or **statistical revisions**.

### Selected indicators (your project table)

| Indicator label | Code | Unit | What it measures | Key use in this project |
|---|---|---|---|---|
| GDP per capita, current prices | NGDPDPC | U.S. dollars per capita | Nominal GDP divided by population. | First view of average income level, but affected by price levels. |
| GDP based on PPP, share of world | PPPSH | Percent of World | Share of world output using purchasing power parity weights. | Compare global economic weight while reducing price-level distortions. |
| GDP per capita, current prices (PPP) | PPPPC | International dollars per capita | GDP per capita adjusted by PPP (more comparable living standards). | Best for cross-country “who lives better” comparisons. |
| Unemployment rate | LUR | Percent of labor force | Unemployed people as a % of total labor force. | Identify where unemployment is highest and how it evolves. |
| General government gross debt | GGXWDG_NGDP | % of GDP | Total government liabilities requiring future repayment, expressed as % of GDP. | Compare public debt burden across countries. |
| Inflation rate, average consumer prices | PCPIEPCH | Annual percent change | Inflation measured by the percent change in a CPI-based basket over time. | Track inflation pressure and compare inflation dynamics. |

### Indicator notes (essential explanations)

#### Inflation rate, average consumer prices (PCPIEPCH)
- **Definition:** Annual percent change in the average CPI.
- **Meaning:** How fast consumer prices increase on average over a year.
- **Source reference:** WEO (October 2025).

Helpful link:
- https://www.imf.org/external/datamapper/PCPIEPCH@WEO/OEMDC/ADVEC/WEOWORLD

#### Unemployment rate (LUR)
- **Definition:** Unemployed persons as a percentage of the total labor force.
- **Source reference:** WEO (October 2025).

Helpful links:
- https://www.imf.org/external/datamapper/LUR@WEO/OEMDC/ADVEC/WEOWORLD
- Excel export example:
  - https://www.imf.org/external/datamapper/LUR@WEO/OEMDC/ADVEC/WEOWORLD/FRA#:~:text=All%20Country%20Data-,EXCEL,-FILE

#### General government gross debt (GGXWDG_NGDP)
Gross debt includes liabilities that require future payment of interest and or principal (e.g., currency and deposits, debt securities, loans, insurance and pension schemes, and other accounts payable). It excludes equity and investment fund shares, financial derivatives, and employee stock options.

- **Unit:** Percent of GDP
- Helpful link:
  - https://www.imf.org/external/datamapper/GGXWDG_NGDP@WEO/OEMDC/ADVEC/WEOWORLD/FRA

#### GDP and PPP (NGDPD, NGDPDPC, PPPPC, PPPSH)
- **Nominal GDP (NGDPD):** Total output valued at current prices.
- **Nominal GDP per capita (NGDPDPC):** Nominal GDP divided by population.
- **PPP concept:** A conversion rate that equalizes purchasing power across countries, allowing comparisons “as if” prices were the same everywhere.
- **PPP per capita (PPPPC):** A strong proxy for comparing living standards across countries.
- **PPP share of world (PPPSH):** A macro-level measure of global economic weight.

### API (how to retrieve the data)
The IMF DataMapper provides a simple HTTP API to retrieve time series by indicator.

#### Base endpoints
- **Indicators API (example):** https://www.imf.org/external/datamapper/api/v1/PPPPC
- **Countries list:** https://www.imf.org/external/datamapper/api/v1/countries

#### Typical workflow
1. Choose the **indicator code** (e.g., `NGDPD`, `PPPPC`, `LUR`).
2. Get the **country codes** you want (from the countries endpoint).
3. Request the series using the indicator endpoint.
4. Parse the returned JSON into a table for analysis.

> **Why use the API instead of manual export?**
> - Automates updates and refreshes.
> - Enables reproducible pipelines.
> - Supports collecting multiple indicators consistently.

### Links (quick access)
- WEO dataset hub: https://www.imf.org/external/datamapper/datasets/WEO
- GDP (NGDPD): https://www.imf.org/external/datamapper/NGDPD@WEO/OEMDC/ADVEC/WEOWORLD/FRA
- GDP per capita (NGDPDPC): https://www.imf.org/external/datamapper/NGDPDPC@WEO/OEMDC/ADVEC/WEOWORLD
- PPP share of world (PPPSH): https://www.imf.org/external/datamapper/PPPSH@WEO/OEMDC/ADVEC/WEOWORLD
- PPP per capita (PPPPC): https://www.imf.org/external/datamapper/PPPPC@WEO/OEMDC/ADVEC/WEOWORLD
- Unemployment (LUR): https://www.imf.org/external/datamapper/LUR@WEO/OEMDC/ADVEC/WEOWORLD
- Debt (GGXWDG_NGDP): https://www.imf.org/external/datamapper/GGXWDG_NGDP@WEO/OEMDC/ADVEC/WEOWORLD/FRA
- Inflation (PCPIEPCH): https://www.imf.org/external/datamapper/PCPIEPCH@WEO/OEMDC/ADVEC/WEOWORLD
