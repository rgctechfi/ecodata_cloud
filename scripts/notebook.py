# %%
import pandas as pd
from pathlib import Path

debt = r'/Users/rgctechfi/Projects/ecodata_cloud/data/raw/debt/general_government_gross_debt_20260318.xls'

# %%
# Loaded variable 'df' from URI: /Users/rgctechfi/Projects/ecodata_cloud/data/raw/debt/general_government_gross_debt_20260318.xls
debt_path = Path(debt)
if debt_path.suffix.lower() == ".xlsx":
    df_debt = pd.read_excel(debt_path, engine="openpyxl")
else:
    # .xls is legacy; read with xlrd then save as .xlsx for openpyxl workflows
    df_debt = pd.read_excel(debt_path, engine="xlrd", engine_kwargs={"ignore_workbook_corruption": True})
    debt_xlsx = debt_path.with_suffix(".xlsx")
    df_debt.to_excel(debt_xlsx, index=False, engine="openpyxl")


# %%
df_debt.head()

# %%
df_debt.describe()
# %%
