import pandas as pd
import shutil

# 設定
PREF_CODE = "160008"
PREF_NAME = "富山県"
CITY_NAME = ""
URL = "https://toyama-pref.box.com/shared/static/8wb3518lh47jx8iv81f80jsmqnnlw46m.xlsx"

"""
患者等発生状況
"""
df_patients = pd.read_excel(
    URL,
    sheet_name="患者等発生状況",
    index_col="No",
    dtype={"発症日": "object", "年代": "object", "備考": "object"},
)
df_patients["発症日"]=df_patients["発症日"].apply(lambda x : str(x).replace("nan","").replace(" 00:00:00",""))
df_patients.to_csv("./data/toyama_patients.csv", encoding="utf_8_sig")

df_patients.rename(
    columns={
        "検査結果判明日": "公表_年月日",
        "発症日": "発症_年月日",
        "居住地": "患者_居住地",
        "年代": "患者_年代",
        "性別": "患者_性別",
        "職業": "患者_職業",
        "症状": "患者_状態",
        "渡航歴の有無": "患者_渡航歴の有無フラグ",
        "状態": "患者_退院済フラグ",
    },
    inplace=True,
)

df_patients["全国地方公共団体コード"] = PREF_CODE
df_patients["都道府県名"] = PREF_NAME
df_patients["市区町村名"] = CITY_NAME

df_patients["患者_退院済フラグ"] = (
    df_patients["患者_退院済フラグ"].replace({"入院中": 0, "入院調整中": 0, "入院": 0, "退院": 1, "死亡": 1, "調査中": None}).astype("Int64")
)

df_patients["患者_渡航歴の有無フラグ"] = (
    df_patients["患者_渡航歴の有無フラグ"].replace({"x": 0, "o": 1}).astype("Int64")
)

df_patients["患者_症状"] = ""

df_patients["患者_年代"] = df_patients["患者_年代"].replace({"90代以上": "90歳以上"})

patients = df_patients.loc[
    :,
    [
        "全国地方公共団体コード",
        "都道府県名",
        "市区町村名",
        "公表_年月日",
        "発症_年月日",
        "患者_居住地",
        "患者_年代",
        "患者_性別",
        "患者_職業",
        "患者_状態",
        "患者_症状",
        "患者_渡航歴の有無フラグ",
        "患者_退院済フラグ",
        "備考",
    ],
]

patients.to_csv(
    "./data/160008_toyama_covid19_patients.csv",
    index=False,
    encoding="utf_8_sig",
)

"""
日別集計
"""
df_counts = pd.read_excel(
    URL,
    sheet_name="日別集計",
    engine="openpyxl",
    index_col="年月日",
    parse_dates=True,
    dtype={
        "PCR検査数": "Int64",
        "抗原検査数": "Int64",
        "陰性人数": "Int64",
        "陽性人数": "Int64",
        "一般相談件数": "Int64",
        "受診・相談センター相談件数": "Int64",
        "退院者数": "Int64",
        "死亡者数": "Int64",
        "備考": "object",
    }
    )
df_counts.to_csv("./data/toyama_counts.csv", encoding="utf_8_sig")

# 検査実施人数
df_counts["実施_年月日"] = df_counts.index.strftime("%Y-%m-%d")

# 陰性確認数
df_counts["完了_年月日"] = df_counts.index.strftime("%Y-%m-%d")

# コールセンター相談件数
df_counts["受付_年月日"] = df_counts.index.strftime("%Y-%m-%d")

df_counts["全国地方公共団体コード"] = PREF_CODE
df_counts["都道府県名"] = PREF_NAME
df_counts["市区町村名"] = CITY_NAME

# 検査実施人数
test_people = df_counts.loc[
    :, ["実施_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "PCR検査数", "備考"]
].copy()

test_people.rename(columns={"PCR検査数": "検査実施_人数"}, inplace=True)

test_people.to_csv(
    "./data/160008_toyama_covid19_test_people.csv",
    index=False,
    encoding="utf_8_sig",
)

antigen_test_people = df_counts.loc[
    :, ["実施_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "抗原検査数", "備考"]
].copy()

antigen_test_people.rename(columns={"抗原検査数": "検査実施_人数"}, inplace=True)

antigen_test_people.to_csv(
    "./data/160008_toyama_covid19_antigen_test_people.csv",
    index=False,
    encoding="utf_8_sig",
)

# 陰性確認数
df_counts.rename(columns={"退院者数": "陰性確認_件数"}, inplace=True)

confirm_negative = df_counts.loc[
    :, ["完了_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "陰性確認_件数", "備考"]
].copy()

confirm_negative.to_csv(
    "./data/160008_toyama_covid19_confirm_negative.csv",
    index=False,
    encoding="utf_8_sig",
)

# 入退院確認数
df_counts.rename(columns={"陽性人数": "陽性確認_件数", "死亡者数": "死亡確認_件数"}, inplace=True)

confirm_patients = df_counts.loc[
    :,
    ["完了_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "陽性確認_件数", "陰性確認_件数", "死亡確認_件数", "備考"],
].copy()

confirm_patients.to_csv(
    "./data/160008_toyama_covid19_confirm_patients.csv",
    index=False,
    encoding="utf_8_sig",
)

# 一般相談件数
call_center = df_counts.loc[
    :, ["受付_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "一般相談件数"]
].copy()

call_center.rename(columns={"一般相談件数": "相談件数"}, inplace=True)
call_center.to_csv(
    "./data/160008_toyama_covid19_call_center.csv",
    index=False,
    encoding="utf_8_sig",
)

# 帰国者・接触者相談センター相談件数
hot_line = df_counts.loc[
    :, ["受付_年月日", "全国地方公共団体コード", "都道府県名", "市区町村名", "受診・相談センター相談件数"]
].copy()

hot_line.rename(columns={"受診・相談センター相談件数": "相談件数"}, inplace=True)
hot_line.to_csv(
    "./data/160008_toyama_covid19_hot_line.csv",
    index=False,
    encoding="utf_8_sig",
)

shutil.make_archive("./opendata", "zip", root_dir="./data")
