import sys
from pathlib import Path
import polars as pl


def get_unique_and_statistics(df, where="dataframe"):
    """
    Print the total and unique row counts of the dataframe.
    Returns the unique dataframe.
    """
    total_rows = df.select(pl.len()).collect().item()
    unique_df = df.unique()
    unique_rows = unique_df.select(pl.len()).collect().item()

    print(f"Number of rows {where}: {total_rows}")
    print(f"Number of unique rows {where}: {unique_rows}")
    return unique_df


def format_and_save(df, output_path):
    """
    Formats the dataframe:
    - Converts 'batch' to YYYY-MM-DD.
    - Replaces nulls with 'NA'.
    - Reorders columns as specified.
    - Saves to the provided output path.
    """
    # Desired column order
    column_order = [
        'specimen_id', 'group', 'background_strain_name', 'colony_id',
        'marker_accession_id', 'allele_accession_id', 'batch', 'metadata_group',
        'weight', 'sex', 'production_center', 'phenotyping_center'
    ]

    # Format batch to date
    df = df.with_columns(
        pl.col("batch").str.slice(0, 10).alias("batch")
    )

    # Fill nulls with "NA"
    df = df.fill_null("NA")

    # Reorder columns
    df = df.select(column_order)

    # Save to CSV
    df.write_csv(output_path)


def main(input_dir):
    """
    Generates control datasets from IMPC observation parquet files.
    """
    input_path = Path(input_dir)
    if not input_path.exists() or not input_path.is_dir():
        print(f"Error: Provided input path '{input_dir}' does not exist or is not a directory.")
        sys.exit(1)

    requested_columns = [
        'allele_accession_id', 'gene_accession_id', 'external_sample_id',
        'biological_sample_group', 'sex', 'production_center', 'colony_id',
        'weight', 'zygosity', 'strain_name', 'date_of_experiment',
        'metadata_group', 'phenotyping_center'
    ]

    output_dir = Path("output_control")
    output_dir.mkdir(exist_ok=True)

    # Load and concatenate lazy frames.
    lazy_frames = [
        pl.scan_parquet(str(file)).select(requested_columns)
        for file in input_path.glob("*.parquet")
    ]
    lazy_df = pl.concat(lazy_frames)

    total_rows = lazy_df.select(pl.len()).collect().item()
    print(f"Total number of rows: {total_rows}")

    # Process rows with unknown colony_id.
    unknown_df = (
        lazy_df
        .filter(pl.col("colony_id") == "unknown")
        .rename({
            'external_sample_id': 'specimen_id',
            'biological_sample_group': 'group',
            'strain_name': 'background_strain_name',
            'date_of_experiment': 'batch',
            'gene_accession_id': 'marker_accession_id'
        })
    )

    unique_unknown_df = get_unique_and_statistics(unknown_df, "with unknown colony_id").collect()

    format_and_save(unique_unknown_df, output_dir / "control_unknown.csv")

    # Process rows with known colony_id.
    filtered_df = lazy_df.filter(pl.col("colony_id") != "unknown")

    # Process control group.
    control_df = (
        filtered_df
        .filter(pl.col("biological_sample_group") == "control")
        .drop(["allele_accession_id", "gene_accession_id"])
    )

    control_df = get_unique_and_statistics(control_df, "in control group")

    # Process experimental group.
    experimental_df = (
        filtered_df
        .filter(pl.col("biological_sample_group") == "experimental")
        .select(["allele_accession_id", "gene_accession_id", "colony_id"])
    )

    experimental_df = get_unique_and_statistics(experimental_df, "in experimental group")

    # Join control and experimental data.
    joined_df = control_df.join(experimental_df, on="colony_id", how="left").rename({
        'external_sample_id': 'specimen_id',
        'biological_sample_group': 'group',
        'strain_name': 'background_strain_name',
        'date_of_experiment': 'batch',
        'gene_accession_id': 'marker_accession_id'
    })

    get_unique_and_statistics(joined_df, "in joined dataframe")

    out_joined_df = joined_df.collect()

    format_and_save(out_joined_df, output_dir / "control_data.csv")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 generate_control_data.py path/to/observations_parquet")
        sys.exit(1)

    input_directory = sys.argv[1]
    main(input_directory)
