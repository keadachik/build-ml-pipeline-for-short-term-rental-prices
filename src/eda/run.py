#!/usr/bin/env python3
"""
EDA step for NYC Airbnb pipeline
"""
import argparse
import wandb
import pandas as pd
import ydata_profiling


def go(args):
    """
    Execute EDA step
    """
    # Initialize W&B run
    run = wandb.init(project="nyc_airbnb", group="eda", save_code=True)
    
    # Fetch artifact from W&B
    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)
    
    # Create profile report for original data
    df_original = df.copy()
    profile_original = ydata_profiling.ProfileReport(df_original)
    profile_original.to_file("report_original.html")
    
    # Data cleaning - create a copy for cleaned data
    df_cleaned = df_original.copy()
    
    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df_cleaned['price'].between(min_price, max_price)
    df_cleaned = df_cleaned[idx].copy()
    
    # Convert last_review to datetime
    df_cleaned['last_review'] = pd.to_datetime(df_cleaned['last_review'])
    
    print(f"Original dataset shape: {df_original.shape}")
    print(f"Cleaned dataset shape: {df_cleaned.shape}")
    print(f"Removed {df_original.shape[0] - df_cleaned.shape[0]} outliers")
    
    # Create profile report for cleaned data
    profile_cleaned = ydata_profiling.ProfileReport(df_cleaned)
    profile_cleaned.to_file("report_cleaned.html")
    
    print("Created profile report for cleaned data: report_cleaned.html")
    
    # Save cleaned data
    df_cleaned.to_csv("sample_cleaned.csv", index=False)
    
    # Create artifact for cleaned data
    artifact = wandb.Artifact(
        args.output_artifact,
        type="clean_data",
        description="Cleaned sample data with outliers removed and date conversion"
    )
    artifact.add_file("sample_cleaned.csv")
    run.log_artifact(artifact)
    
    print(f"Cleaned dataset shape: {df_cleaned.shape}")
    print(f"Price range: {df_cleaned['price'].min()} - {df_cleaned['price'].max()}")
    print("Uploaded cleaned data to W&B")
    
    # Terminate the W&B run
    run.finish()
    
    print("W&B run finished successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EDA step for NYC Airbnb pipeline")
    
    parser.add_argument("input_artifact", type=str, help="Input artifact name")
    parser.add_argument("output_artifact", type=str, help="Output artifact name")
    parser.add_argument("min_price", type=float, help="Minimum price threshold")
    parser.add_argument("max_price", type=float, help="Maximum price threshold")
    
    args = parser.parse_args()
    
    go(args)
