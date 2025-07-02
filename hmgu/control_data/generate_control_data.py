#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import polars as pl


# In[2]:


# Calculate number of rows in initial dataframe, return df with unique rows.
def get_unique_and_statistics(df, where="dataframe"):
    df_unique = df.unique()
    print(f"Number of rows in {where}: {df.select(pl.len()).collect().item()}")
    print(f"Number of unique rows in {where}: {df_unique.select(pl.len()).collect().item()}")
    return df_unique
    
requested_columns = [
    'allele_accession_id',
    'gene_accession_id',
    'external_sample_id',
    'biological_sample_group',
    'sex',
    'production_center',
    'colony_id',
    'weight',
    'zygosity',
    'strain_name',
    'date_of_experiment',
    'metadata_group',
    'phenotyping_center'
]

# Input files are stored on SLURM: ${KOMP_PATH}/data-releases/latest-input/dr23.0/output/observations_parquet 
input_path = Path("observations_parquet")
output_dir = Path("output_control/")
output_dir.mkdir(exist_ok=True)


# In[3]:


# Prepare lazy frames for all parquet files.
lazy_frames = [
    pl.scan_parquet(str(file)).select(requested_columns)
    for file in input_path.glob("*.parquet")
]

# Concatenate all lazy frames.
lazy_df = pl.concat(lazy_frames)

total_rows = lazy_df.select(pl.len()).collect().item()
print(f"Total number of rows: {total_rows}")

# Confirm that allele_accession_id and colony_id pairs are unique.
allele_rows = (
    lazy_df
    .filter(pl.col("allele_accession_id").is_not_null())
    .select(["allele_accession_id", "gene_accession_id", "colony_id"])
    .unique()
)

# Count unique rows after removing duplicates.
unique_allele_rows_count = allele_rows.unique().select(pl.len()).collect().item()
print(f"Number of unique allele rows: {unique_allele_rows_count}")

# Count number of unique allele_accession_id values.
unique_allele_ids_count = allele_rows.select(pl.col("colony_id").n_unique()).collect().item()
print(f"Number of unique colony_id values: {unique_allele_ids_count}")


# In[4]:


# Extract the dataframe where colony_id == 'unknown'.
unknown_df = (
    lazy_df
    .filter(pl.col("colony_id") == "unknown")
    .rename({
        'external_sample_id': 'specimen_id',
        'biological_sample_group': 'group',
        'strain_name': 'background_strain_name',
        'date_of_experiment': 'batch'
    })
)
unknown_df = get_unique_and_statistics(unknown_df, "with unknown colony_id")
unique_unknown_df = unknown_df.collect()
unique_unknown_df.write_csv(output_dir / "control_unknown.csv")


# In[5]:


# Filter out rows where colony_id == 'unknown'.
filtered_df = lazy_df.filter(pl.col("colony_id") != "unknown")

# Select rows where biological_sample_group is control.
control_df = (
    filtered_df
    .filter(pl.col("biological_sample_group") == "control")
)
control_df = control_df.drop(["allele_accession_id", "gene_accession_id"])
control_df = get_unique_and_statistics(control_df, "in control group")

# Select rows where biological_sample_group is experimental.
experimental_df = (
    filtered_df
    .filter(pl.col("biological_sample_group") == "experimental")
    .select(["allele_accession_id", "gene_accession_id", "colony_id"])
)
experimental_df = get_unique_and_statistics(experimental_df, "in experimental group")

# Generate output dataframe.
joined_df = control_df.join(
    experimental_df, 
    on="colony_id", 
    how="left"
)

joined_df = joined_df.rename({
    'external_sample_id': 'specimen_id',
    'biological_sample_group': 'group',
    'strain_name': 'background_strain_name',
    'date_of_experiment': 'batch'
    })

get_unique_and_statistics(joined_df, "in joined dataframe")

out_joined_df = joined_df.collect()
out_joined_df.write_csv(output_dir / "control_data.csv")


# In[6]:


# Print columns.
out_joined_df.collect_schema()  


# In[7]:


# Show first 3 rows.
head_rows = joined_df.limit(3).collect()
head_rows

