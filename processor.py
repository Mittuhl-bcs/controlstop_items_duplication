# importing libraries
import pandas as pd
import numpy as np
import re
import argparse
import sys
import os
import glob
import logging
from datetime import datetime
import json



# class for this
class PBmapper():
    prefix_name = {"75F": "75F",
                   "RES": "ADEMCO, INC. (Resideo)",
                   "ALR": "ALERTON NOVAR",
                   "ALT": "Altech Corporation",
                   "APF": "Apollo-Fire",
                   "ASC": "ASCO L.P.",
                   "ASI": "ASI Controls",
                   "ACI": "Automation Components Inc (ACI)",
                   "BEL": "BELIMO AIRCONTROLS (USA), INC",
                   "BWR": "Best Wire",
                   "BAP": "BUILDING AUTOMATION PRODUCTS, INC. (BAPI)",
                   "CCS": "Contemporary Control Systems",
                   "ISC": "Controlli",
                   "DIS": "DISTECH CONTROLS",
                   "DIV": "DIVERSITECH CORPORATION",
                   "DWY": "DWYER INSTRUMENTS, INC.",
                   "FIE": "FIREYE INC.",
                   "FND": "FUNCTIONAL DEVICES, INC.",
                   "CNA": "Genuine Cable Group (GCG) (Connect Air)",
                   "GDR": "GOODRICH SALES INC.",
                   "HTM": "HEAT-TIMER CORP",
                   "HFE": "HOFFMAN ENCLOSURES INC.",
                   "HWW": "HONEYWELL INC.",
                   "HWI": "HONEYWELL INTERNATIONAL ECC US (HOFS)",
                   "HWT": "HONEYWELL THERMAL SOLUTIONS",
                   "ICM": "ICM",
                   "IDC": "IDEC CORPORATION",
                   "ICS": "Industrial Connections & Solutions, LLC",
                   "JCI": "Johnson Controls Inc",
                   "KLN": "Klein Tools, Inc.",
                   "KMC": "Kreuter (KMC) Controls",
                   "LUM": "Lumen Radio",
                   "LYX": "LynxSpring Inc.",
                   "MCO": "Macurco",
                   "MXC": "Maxicap",
                   "MAX": "MAXITROL COMPANY",
                   "MXL": "Maxline",
                   "NCG": "NU-CALGON WHOLESALER",
                   "PHX": "Phoenix Contact USA, Inc.",
                   "PLN": "PROLON",
                   "RBS": "ROBERTSHAW CONTROLS COMPANY",
                   "SAG": "SAGINAW CONTROL & ENGINEERING",
                   "SCH": "SCHNEIDER ELECTRIC BUILDINGS AMERICAS, INC",
                   "SEI": "Seitron",
                   "SEN": "SENVA, INC.",
                   "SET": "SETRA SYSTEMS, INC.",
                   "SIE": "SIEMENS INDUSTRY, INC.",
                   "SKY": "Skyfoundry",
                   "SYS": "System Sensor",
                   "TOS": "TOSIBOX, INC.",
                   "VYK": "Tridium Inc.",
                   "USM": "US Motor Nidec Motor Corp",
                   "HWA": "VULCAIN ALARM DIVISION",
                   "XYL": "Xylem",
                   "YRK": "York Chiller Parts",
                   "PFP": "Performance Pipe",
                   "PER": "Periscope",
                   "JNL": "J&L Manufacturing",
                   "RFL": "NiagaraMod",
                   "FUS": "Fuseco",
                   "J2I": "J2Inn",
                   "MAX": "MAXITROL COMPANY",
                   "SAS": "Spreecher & Shuh",
                   "SCC": "Siemens Combustion (SCC)",
                   "PIE": "Pietro",
                   "IOH": "IO HVAC"
                   }

    # function for reading the files
    def read_files(self, company_path, pricing_path):

        print(f"This {company_path}, {pricing_path}")
        company_df = pd.read_excel(company_path, sheet_name= "Worked", engine="openpyxl")
        pricing_df = pd.read_excel(pricing_path, sheet_name= "Worked",  engine="openpyxl")
        logging.info("Files are read into dataframes")

        return company_df, pricing_df

    # function for initiating columns
    def column_initiator(self, company_df, pricing_df):
        company_df["Stripped_supplier_PN"] = ""
        company_df["Matched_pricingdoc_SPN"]  = ""
        company_df["Matching_status"]  = ""
        
        pricing_df["Stripped_supplier_PN"] = ""
        pricing_df["Matched_companydoc_SPN"] = ""
        pricing_df["Matching_status"]  = "Not Matched"

        logging.info("New columns initiated in both the files")


        return company_df, pricing_df

    # needs commenting of the prefix
    # needs the spl stripping function as well
    def modifier(self, company_df, pricing_df):
   
                
        for index, row in company_df.iterrows():
            #print(row)
            sspart_no = row["Supplier_part_no"]
            company_df.loc[index, "Stripped_supplier_PN"] = re.sub(r'[^a-zA-Z0-9\s]', "", sspart_no)

            
        pricing_df.reset_index(drop=True, inplace=True)

        #print(pricing_df.head())
        for index, row in pricing_df.iterrows():
            pricing_df.loc[index, "Stripped_supplier_PN"] = re.sub(r'[^a-zA-Z0-9\s]', "", str(row["Supplier_part_no"]))

        print(pricing_df.head())

        return company_df, pricing_df
    

    # function for implementing logic
    def matching_logic(self, company_df, pricing_df):

        # iterring over company df
        for index, row in company_df.iterrows():
            sspn = row["Stripped_supplier_PN"]     # replacement: should be item_id instead of supplier_part_no
            
            match_check = 0
            for i, r in pricing_df.iterrows():
                if r["Stripped_supplier_PN"] == sspn:
                    row["Matched_companydoc_SPN"] = r["Supplier_part_no"]
                    r["Matched_pricingdoc_SPN"] = row["Supplier_part_no"]
                    match_check = 1
                    
                    r["Matching_status"] = "Matched"
                    
                    break

            if match_check == 1:
                row["Matching_status"] = "Matched"
            else:
                row["Matching_status"] = "Not Matched"

        return company_df, pricing_df

           
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("--p21_path", help="Give the master folder path")
    parser.add_argument("--controlstop_path", help= "Give the finished list of companies json file path")
    args = parser.parse_args()


    company_df = args.p21_path
    pricing_df = args.controlstop_path

    mapper = PBmapper()
    P21_files, controlstopfiles = mapper.read_files(company_df, pricing_df)
    P21_files, controlstopfiles = mapper.column_initiator(P21_files, controlstopfiles)
    P21_files, controlstopfiles = mapper.modifier(P21_files, controlstopfiles)

    print("p21 data")
    print(P21_files.head())
    
    print("controlstop data")
    print(controlstopfiles.head())

    P21_files, controlstopfiles = mapper.matching_logic(P21_files, controlstopfiles)

    P21_files.to_excel("p21_matched.xlsx", index = False)
    controlstopfiles.to_excel("controlstop_matched.xlsx", index = False)
