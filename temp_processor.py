import pandas as pd
import re


df = pd.read_excel("D:\\Controlstop_items_mapping\\Belimo_p21_items.xlsx", sheet_name="Worked")

for index, row in df.iterrows():
    #print(row)
    sspart_no = row["Supplier_part_no"]
    df.loc[index, "Stripped_supplier_PN"] = re.sub(r'[^a-zA-Z0-9\s]', "", sspart_no)

df.to_excel("Belimo_p21_items.xlsx", index= False)