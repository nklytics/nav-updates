import requests
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

current_date = datetime.now(ZoneInfo("Asia/Kolkata")).date()
rng_date = pl.date_range(
    start=current_date - timedelta(days=3),
    end=current_date - timedelta(days=1),
    eager=True,
).dt.strftime("%d-%b-%Y")

headers = {
    "Content-Type": "text/plain",
    "Content-Encoding": "gzip",
    "Host": "portal.amfiindia.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
}


def get_nav(selected_day):
    # url='https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt=25-May-2023'
    url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt={selected_day}"
    response = requests.get(url=url, headers=headers)
    txts = response.text.split("\r\n")
    df = pl.DataFrame(txts)
    df = df.filter((pl.col("column_0") != "") & (pl.col("column_0").str.contains(";")))
    df = df.select(pl.col("column_0").str.split(";"))
    cols = df["column_0"].to_list()[0]
    data = pl.DataFrame(df["column_0"].to_list()[1:], schema=cols)
    data = data.with_columns(
        [
            pl.col("Scheme Code").cast(pl.Int64, strict=False),
            pl.col("Net Asset Value").cast(pl.Float64, strict=False),
            pl.col("Repurchase Price").cast(pl.Float64, strict=False),
            pl.col("Sale Price").cast(pl.Float64, strict=False),
            pl.col("Date").str.to_datetime(format="%d-%b-%Y").cast(pl.Date),
        ]
    )
    # print(selected_day)
    return data


with ThreadPoolExecutor() as executor:
    result = executor.map(get_nav, rng_date)
data = [r for r in result]
# print('all done')

df = pl.concat(data)

df = df.with_columns(
    pl.when(pl.col(pl.Utf8) == "")
    .then(None)
    .otherwise(pl.col(pl.Utf8))  # keep original value
    .keep_name()
)

# Data Update
data = df.drop(["Scheme Name", "ISIN Div Payout/ISIN Growth", "ISIN Div Reinvestment"])
old_data = pl.read_parquet("NAV 2023(zstd-12).parquet")
pl.concat([old_data, data]).unique().sort(by="Date").write_parquet(
    "NAV 2023(zstd-12).parquet",
    compression="zstd",
    compression_level=12,
)

# Code Update
codes_update = df[["Scheme Code", "Scheme Name"]].unique()
old_codes = pl.read_parquet("scheme_code.parquet")
pl.concat([old_codes, codes_update]).unique().write_parquet(
    "scheme_code.parquet", compression="zstd", compression_level=19
)
