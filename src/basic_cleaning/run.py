#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in W&B
"""

import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(
    level=logging.INFO, format="%(asctime)-15s - %(levelname)-8s - %(message)s"
)
logger = logging.getLogger()


def go(args):
    logger.info("Starting basic cleaning step")
    run = wandb.init(project="nyc_airbnb", job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    df = pd.read_csv(artifact_local_path)
    df = df[(df.price >= args.min_price) & (df.price <= args.max_price)]
    idx = df["longitude"].between(-74.25, -73.50) & df["latitude"].between(40.5, 41.2)
    df = df[idx].copy()
    df.to_csv(args.output_artifact, index=False)
    logger.info("Cleaned data saved to %s", args.output_artifact)
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    # make sure the artifact is uploaded before the script ends
    # artifact.wait()
    logger.info("Logged artifact %s to W&B", args.output_artifact)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This step cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Original dataset",
        required=True,
        default="sample.csv:latest",
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Cleaned dataset",
        required=True,
        default="clean_sample.csv",
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of the output artifact",
        required=True,
        default="cleaned_data",
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the cleaned dataset",
        required=True,
        default="Data after basic cleaning",
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price to filter the dataset",
        required=True,
        default=10,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum price to filter the dataset",
        required=True,
        default=350,
    )

    args = parser.parse_args()

    go(args)
