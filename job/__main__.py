"""
Main module of the weeklt-insta-job project.

"""

import configparser
import logging
import os
from datetime import datetime

import pandas as pd
from ig_helpers import (
    format_folder_path,
    get_account_info,
    get_media_comments,
    get_media_id,
    get_media_info,
    get_media_metrics,
    write_to_s3,
)
from rich.logging import RichHandler
from rich.traceback import install

pd.set_option("display.max_columns", None)
FORMAT = "[%(levelname)s] [%(message)s]"
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt="[%Y-%m-%d %H:%M:%S]",
    handlers=[RichHandler(show_level=False, show_path=False)],
)
install()
logger = logging.getLogger("main")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.realpath(".."), "setup.cfg"))
    logger.info("STARTING JOB")
    ig_user_id = config["GRAPH_API"]["IG_USER_ID"]
    base = config["GRAPH_API"]["BASE"]
    username = config["GRAPH_API"]["USERNAME"]
    access_token = config["GRAPH_API"]["ACCESS_TOKEN"]
    data_lake_bucket_name = config["S3"]["DATALAKE_BUCKET_NAME"]
    user_node = f"/{ig_user_id}"
    logger.info(f"GETTING DATA FROM INSTA USERNAME: @{username}")
    date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"REQUESTING ACCOUNT DATA ...")
    account_info = get_account_info(base, user_node, access_token)
    account_info_df = pd.DataFrame([account_info])
    account_info_bucket_name = f"{data_lake_bucket_name}/AccountInfo"
    write_to_s3(
        account_info_df,
        format_folder_path(account_info_bucket_name, date_str, logger),
        config["AWS"],
        logger,
    )

    logger.info(f"REQUESTING MEDIA DATA ...")
    media_ids = get_media_id(base, user_node, access_token)
    media_id_list = [media_id["id"] for media_id in media_ids["data"]]
    media_info = get_media_info(base, media_id_list, access_token)
    media_info_df = pd.DataFrame(media_info)
    media_info_bucket_name = f"{data_lake_bucket_name}/MediaInfo"
    write_to_s3(
        media_info_df,
        format_folder_path(media_info_bucket_name, date_str, logger),
        config["AWS"],
        logger,
    )

    logger.info(f"REQUESTING MEDIA METRICS ...")
    media_metrics = get_media_metrics(base, media_id_list, access_token)
    media_metrics_list = []
    for media_metric in media_metrics:
        media_metrics_dict = dict()
        media_id = media_metric["data"][0]["id"].split("/")[0]
        media_metrics_dict["media_id"] = media_id
        for metric in media_metric["data"]:
            media_metrics_dict[metric["name"]] = metric["values"][0]["value"]
        media_metrics_list.append(media_metrics_dict)
    media_metrics_df = pd.DataFrame(media_metrics_list)
    media_metrics_bucket_name = f"{data_lake_bucket_name}/MediaMetrics"
    write_to_s3(
        media_metrics_df,
        format_folder_path(media_metrics_bucket_name, date_str, logger),
        config["AWS"],
        logger,
    )

    logger.info(f"REQUESTING MEDIA COMMENTS ...")
    media_comments = get_media_comments(base, media_id_list, access_token)
    media_comments_list = []
    for media_comments_key, media_comments_value in media_comments.items():
        if media_comments_value["data"]:
            for comments in media_comments_value["data"]:
                media_comments_dict = dict()
                media_comments_dict["media_id"] = media_comments_key
                media_comments_dict.update(comments)
                media_comments_list.append(media_comments_dict)
    media_comments_df = pd.DataFrame(media_comments_list)
    media_comments_bucket_name = f"{data_lake_bucket_name}/MediaComments"
    write_to_s3(
        media_comments_df,
        format_folder_path(media_comments_bucket_name, date_str, logger),
        config["AWS"],
        logger,
    )

    logger.info("END OF JOB")
