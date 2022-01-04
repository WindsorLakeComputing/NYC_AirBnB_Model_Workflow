#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import os
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info("Dropping outliers")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    logger.info("Converting last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(args.output_artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="raw data to be cleansed",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="cleansed sample data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="type for the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="the refined data",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum cost of a rental",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The maximum cost of a rental",
        required=True
    )


    args = parser.parse_args()

    go(args)
