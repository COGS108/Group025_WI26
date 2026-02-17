import os
import pandas as pd

RAW = "data/00-raw"
OUT = "data/01-interim"

os.makedirs(OUT, exist_ok=True)

demo = pd.read_sas(f"{RAW}/DEMO_J.XPT")
ghb  = pd.read_sas(f"{RAW}/GHB_J.XPT")
glu  = pd.read_sas(f"{RAW}/GLU_J.XPT")
hiq  = pd.read_sas(f"{RAW}/HIQ_J.XPT")
smq  = pd.read_sas(f"{RAW}/SMQ_J.XPT")
alq  = pd.read_sas(f"{RAW}/ALQ_J.XPT")
paq  = pd.read_sas(f"{RAW}/PAQ_J.XPT")
diq  = pd.read_sas(f"{RAW}/DIQ_J.XPT")

df = demo.merge(ghb, on="SEQN", how="left") \
         .merge(glu, on="SEQN", how="left") \
         .merge(hiq, on="SEQN", how="left") \
         .merge(smq, on="SEQN", how="left") \
         .merge(alq, on="SEQN", how="left") \
         .merge(paq, on="SEQN", how="left") \
         .merge(diq, on="SEQN", how="left")

df = df[df["RIDAGEYR"] >= 18]

df["diabetes_biomarker"] = (df["LBXGH"] >= 6.5) | (df["LBXGLU"] >= 126)
df["diagnosed"] = df["DIQ010"] == 1

df = df[df["diabetes_biomarker"]]

df.to_csv("data/01-interim/nhanes_diabetes.csv", index=False)

print("Dataset saved to data/01-interim/nhanes_diabetes.csv")
print("Rows:", df.shape[0])
print("Columns:", df.shape[1])
