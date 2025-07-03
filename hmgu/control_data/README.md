# How to Generate the Data
1. Copy input files to working directory.
```console
mkdir observations_parquet
cp -r ${KOMP_PATH}/data-releases/latest-input/dr23.0/output/observations_parquet/*.parquet observations_parquet 
```

2. Download script `generate_control_data.py`.
```console
export BRANCH="ctrl"
wget https://raw.githubusercontent.com/mpi2/impc_working_group_data/refs/heads/${BRANCH}/hmgu/control_data/generate_control_data.py
``` 

3. Run SLURM job to generate the data.
```console
sbatch --mem=32g --time=00:10:00 --wrap="python3 generate_control_data.py observations_parquet"
``` 

4. Go to generated folder and compress output.
```console
cd output_control
zip -r ../impc_control_data_dr23.0.zip .
```

5. Move archive to the FTP area.

Script will generate `output_control` folder with two files:
```
output_control
├── control_data.csv
└── control_unknown.csv
```
