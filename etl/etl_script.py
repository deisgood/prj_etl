import xml.etree.ElementTree as ET
import pandas as pd
import requests
import math
import boto3
import json

from dataclasses import dataclass, field
from io import StringIO
from tabulate import tabulate
from datetime import datetime, timedelta
from itertools import product
from pprint import pprint


def get_env_config(path: str):
    config = None
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config


env = "dev"
s3_bucket = "public-data-01"
page_size = 10

env_config = get_env_config(path=f"env/{env}_config.json")
end_point = env_config.get("end_point")
service_key = env_config.get("service_key")

aws_config = get_env_config(path=f"env/{env}_aws_config.json")
region_name = aws_config.get("region_name")
aws_access_key_id = aws_config.get("aws_access_key_id")
aws_secret_access_key = aws_config.get("aws_secret_access_key")


request_headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0;Win64; x64)\
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98\
        Safari/537.36"
    ),
}

dtypes = {
    "거래금액": "int64",
    "건축년도": "string",
    "년": "string",
    "도로명": "string",
    "도로명건물본번호코드": "string",
    "도로명건물부번호코드": "string",
    "도로명시군구코드": "string",
    "도로명일련번호코드": "string",
    "도로명지상지하코드": "string",
    "도로명코드": "string",
    "법정동": "string",
    "법정동본번코드": "string",
    "법정동부번코드": "string",
    "법정동시군구코드": "string",
    "법정동읍면동코드": "string",
    "법정동지번코드": "string",
    "아파트": "string",
    "월": "string",
    "일": "string",
    "일련번호": "string",
    "전용면적": "string",
    "지번": "string",
    "지역코드": "string",
    "층": "string",
}


@dataclass
class BaseModelPublicData:
    headers: dict
    req_url: str
    response: requests.models.Response = field(init=False, default=None)
    response_text: str = field(init=False, default=None)
    response_status_code: str = field(init=False, default=None)
    params: dict


def get_requests(PublicData):
    try:
        res = requests.get(headers=PublicData.headers,
                           url=PublicData.req_url,
                           params=PublicData.params)
        if res.status_code != 200:
            raise ConnectionError("API 연결 에러")
        return res
    except Exception as e:
        raise e


def get_total_page_count(PublicData):
    try:
        PublicData.response = get_requests(PublicData)
        total_count = 0
        root = ET.fromstring(PublicData.response.text)
        for child in root.find("./body"):
            if child.tag == "totalCount":
                total_count = child.text
                break

        total_page = math.ceil(int(total_count) / page_size)
    except Exception as e:
        raise e
    return total_page


def convert_df_types(df):
    for col in df.columns:
        df[col] = df[col].astype(dtypes.get(col))


def load_to_s3_bucket(df, output_filename):
    try:
        bucket = s3_bucket
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,
                  header=True,
                  encoding="utf-8-sig",
                  index=False)
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        s3_resource.Object(bucket, output_filename).put(Body=csv_buffer.getvalue())
    except Exception as e:
        raise e


def run_etl():
    rows = []
    lawd_cd = "11110"
    deal_ymd = "202301"

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": page_size,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd
    }

    PublicData = BaseModelPublicData(headers=request_headers,
                                     req_url=end_point,
                                     params=params)

    total_page = get_total_page_count(PublicData)

    for i in range(1, total_page + 1):
        params = {
            "serviceKey": service_key,
            "pageNo": i,
            "numOfRows": page_size,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
        }

        PublicData.params = params
        PublicData.response = get_requests(PublicData)

        root = ET.fromstring(PublicData.response.text)

        for child in root.findall("./body/items/"):
            거래금액 = child.find("거래금액").text if child.find("거래금액") is not None else ""
            건축년도 = child.find("건축년도").text if child.find("건축년도") is not None else ""
            년 = child.find("년").text if child.find("년") is not None else ""
            도로명 = child.find("도로명").text if child.find("도로명") is not None else ""
            도로명건물본번호코드 = child.find("도로명건물본번호코드").text if child.find("도로명건물본번호코드") is not None else ""
            도로명건물부번호코드 = child.find("도로명건물부번호코드").text if child.find("도로명건물부번호코드") is not None else ""
            도로명시군구코드 = child.find("도로명시군구코드").text if child.find("도로명시군구코드") is not None else ""
            도로명일련번호코드 = child.find("도로명일련번호코드").text if child.find("도로명일련번호코드") is not None else ""
            도로명지상지하코드 = child.find("도로명지상지하코드").text if child.find("도로명지상지하코드") is not None else ""
            도로명지상지하코드 = child.find("도로명지상지하코드").text if child.find("도로명지상지하코드") is not None else ""
            도로명코드 = child.find("도로명코드").text if child.find("도로명코드") is not None else ""
            법정동 = child.find("법정동").text if child.find("법정동") is not None else ""
            법정동본번코드 = child.find("법정동본번코드").text if child.find("법정동본번코드") is not None else ""
            법정동부번코드 = child.find("법정동부번코드").text if child.find("법정동부번코드") is not None else ""
            법정동시군구코드 = child.find("법정동시군구코드").text if child.find("법정동시군구코드") is not None else ""
            법정동읍면동코드 = child.find("법정동읍면동코드").text if child.find("법정동읍면동코드") is not None else ""
            법정동지번코드 = child.find("법정동지번코드").text if child.find("법정동지번코드") is not None else ""
            아파트 = child.find("아파트").text if child.find("아파트") is not None else ""
            월 = child.find("월").text if child.find("월") is not None else ""
            일 = child.find("일").text if child.find("일") is not None else ""
            일련번호 = child.find("일련번호").text if child.find("일련번호") is not None else ""
            전용면적 = child.find("전용면적").text if child.find("전용면적") is not None else ""
            지번 = child.find("지번").text if child.find("지번") is not None else ""
            지역코드 = child.find("지역코드").text if child.find("지역코드") is not None else ""
            층 = child.find("층").text if child.find("층") is not None else ""

            rows.append(
                {
                    "거래금액": 거래금액.replace(",", ""),
                    "건축년도": 건축년도,
                    "년": 년,
                    "도로명": 도로명,
                    "도로명건물본번호코드": 도로명건물본번호코드,
                    "도로명건물부번호코드": 도로명건물부번호코드,
                    "도로명시군구코드": 도로명시군구코드,
                    "도로명일련번호코드": 도로명일련번호코드,
                    "도로명지상지하코드": 도로명지상지하코드,
                    "도로명코드": 도로명코드,
                    "법정동": 법정동,
                    "법정동본번코드": 법정동본번코드,
                    "법정동부번코드": 법정동부번코드,
                    "법정동시군구코드": 법정동시군구코드,
                    "법정동읍면동코드": 법정동읍면동코드,
                    "법정동지번코드": 법정동지번코드,
                    "아파트": 아파트,
                    "월": 월,
                    "일": 일,
                    "일련번호": 일련번호,
                    "전용면적": 전용면적,
                    "지번": 지번,
                    "지역코드": 지역코드,
                    "층": 층,
                }
            )

    print(f"rows: {len(rows)}")
    df = pd.DataFrame(data=rows, columns=dtypes.keys())

    # print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))

    convert_df_types(df)

    load_to_s3_bucket(df, output_filename="df_tmp.csv")


if __name__ == "__main__":
    run_etl()
