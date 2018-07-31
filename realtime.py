import csv
import logging
import os
import shutil
import time

import boto3
import redis
from web3 import Web3

from ethereumetl.csv_utils import set_max_field_size_limit
from ethereumetl.file_utils import smart_open
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from ethereumetl.jobs.exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy


def build_s3_key(base_file_name, start, end):
    padded_start = str(start).rjust(8, "0")
    padded_end = str(end).rjust(8, "0")
    return "ethereumetl/export/{}/start_block={}/end_block={}/{}_{}_{}.csv".format(
        base_file_name,
        padded_start,
        padded_end,
        base_file_name,
        padded_start,
        padded_end
    )


def split_to_batches(start_incl, end_incl, batch_size):
    """start_incl and end_incl are inclusive, the returned batch ranges are also inclusive"""
    for batch_start in range(start_incl, end_incl + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_incl)
        yield batch_start, batch_end


if __name__ == '__main__':
    GETH_URL = os.environ["GETH_URL"]
    S3_BUCKET = os.environ["S3_BUCKET"]
    REDIS_HOST = os.environ["REDIS_HOST"]
    REDIS_KEY = os.environ["REDIS_KEY"]
    INTERVAL_MINUTES = int(os.environ["INTERVAL_MINUTES"])

    BLOCKS_PATH = "./export/blocks.csv"
    TRANSACTIONS_PATH = "./export/transactions.csv"
    TX_HASHES_PATH = "./export/tx_hashes.csv"
    RECEIPTS_PATH = "./export/receipts.csv"
    LOGS_PATH = "./export/logs.csv"

    provider = Web3.HTTPProvider(GETH_URL)
    w3 = Web3(provider)
    r = redis.Redis(host=REDIS_HOST)

    while True:
        LAST_BLOCK = int(r.get(REDIS_KEY))
        CURRENT_BLOCK = int(w3.eth.getBlock("latest").number - 1)
        BATCH_SIZE = CURRENT_BLOCK - LAST_BLOCK

        if BATCH_SIZE <= 0:
            time.sleep(5 * INTERVAL_MINUTES)
            continue

        t0 = time.time()
        logging.info("Running Ethereum export ETL from {start_block} - {end_block} via: {geth}".format(
            start_block=LAST_BLOCK,
            end_block=CURRENT_BLOCK,
            geth=GETH_URL
        ))

        # Remove existing export
        try:
            shutil.rmtree("./export")
        except FileNotFoundError:
            pass

        # Export blocks and transactions
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=LAST_BLOCK,
            end_block=CURRENT_BLOCK,
            batch_size=BATCH_SIZE,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(GETH_URL, batch=True)),
            max_workers=4,
            item_exporter=blocks_and_transactions_item_exporter(BLOCKS_PATH, TRANSACTIONS_PATH),
            export_blocks=True,
            export_transactions=True
        )
        blocks_and_transactions_job.run()

        # Extract transactions hashes
        set_max_field_size_limit()
        with smart_open(TRANSACTIONS_PATH, "r") as input_file, smart_open(TX_HASHES_PATH, "w") as output_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                output_file.write(row["tx_hash"] + '\n')

        # Export receipts and logs
        with smart_open(TX_HASHES_PATH, 'r') as tx_hashes_file:
            receipts_and_logs_job = ExportReceiptsJob(
                tx_hashes_iterable=(tx_hash.strip() for tx_hash in tx_hashes_file),
                batch_size=BATCH_SIZE,
                batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(GETH_URL, batch=True)),
                max_workers=4,
                item_exporter=receipts_and_logs_item_exporter(RECEIPTS_PATH, LOGS_PATH),
                export_receipts=True,
                export_logs=True
            )
            receipts_and_logs_job.run()

        # Upload to S3
        logging.info("Finished export (took {elapsed}). Uploading results to S3 bucket: s3://{s3_bucket}".format(
            elapsed=time.time() - t0,
            s3_bucket=S3_BUCKET
        ))

        t0 = time.time()
        s3 = boto3.resource("s3")

        s3.Bucket(S3_BUCKET).upload_file(
            BLOCKS_PATH,
            build_s3_key("blocks", LAST_BLOCK, CURRENT_BLOCK)
        )

        s3.Bucket(S3_BUCKET).upload_file(
            TRANSACTIONS_PATH,
            build_s3_key("transactions", LAST_BLOCK, CURRENT_BLOCK)
        )

        s3.Bucket(S3_BUCKET).upload_file(
            RECEIPTS_PATH,
            build_s3_key("receipts", LAST_BLOCK, CURRENT_BLOCK)
        )

        s3.Bucket(S3_BUCKET).upload_file(
            LOGS_PATH,
            build_s3_key("logs", LAST_BLOCK, CURRENT_BLOCK)
        )

        r.set(REDIS_KEY, str(CURRENT_BLOCK + 1))

        # Remove existing export
        try:
            shutil.rmtree("./export")
        except FileNotFoundError:
            pass

        logging.info("Finished upload (took {elapsed}). Waiting {interval} minutes until next run...".format(
            elapsed=time.time() - t0,
            interval=INTERVAL_MINUTES
        ))
        time.sleep(60. * INTERVAL_MINUTES)
