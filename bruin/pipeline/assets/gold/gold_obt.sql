SELECT
  c.country_label,
  c.year,
  usd.gdp_per_capita_usd_usd_per_capita,
  ppp.gdp_per_capita_ppp_international_dollars_per_capita,
  share.gdp_ppp_world_share_percent_world,
  unemp.unemployment_rate_percent_labor_force,
  debt.public_debt_gdp_percent_gdp,
  infl.inflation_avg_consumer_percent_change

FROM `ecodatacloud.ecodatacloud_bq_gold.gold__countries` AS c
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__gdp_per_capita_usd` AS usd 
  ON c.id_countryear = usd.id_countryear
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__gdp_per_capita_ppp` AS ppp 
  ON c.id_countryear = ppp.id_countryear
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__gdp_ppp_world_share` AS share 
  ON c.id_countryear = share.id_countryear
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__unemployment_rate` AS unemp 
  ON c.id_countryear = unemp.id_countryear
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__public_debt_gdp` AS debt 
  ON c.id_countryear = debt.id_countryear
LEFT JOIN `ecodatacloud.ecodatacloud_bq_gold.gold__inflation_avg_consumer` AS infl 
  ON c.id_countryear = infl.id_countryear
