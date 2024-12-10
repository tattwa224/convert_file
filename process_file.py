import os
import pandas as pd
import json
import logging
from typing import Dict

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DATA_FOLDER = 'data'
SPECS_FOLDER = 'specs'
OUTPUT_FOLDER = 'output'

type_mapping = {
    'TEXT': 'object',
    'BOOLEAN': 'bool',
    'INTEGER': 'int64',
    'FLOAT': 'float64',
}


def load_spec_file(spec_file_path: str) -> pd.DataFrame:
    """
    Load and validate the specification file.
    """
    try:
        spec_df = pd.read_csv(spec_file_path)
        logger.info(f"Specification file {spec_file_path} loaded successfully.")
        return spec_df
    except Exception as e:
        logger.error(f"Error loading specification file {spec_file_path}: {e}")
        raise


def parse_data_file(data_file_path: str, spec_df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the data file using the specification.
    """
    column_names = spec_df['column name'].tolist()
    column_widths = spec_df['width'].tolist()
    column_dtypes = spec_df['datatype'].tolist()

    dtypes = {col: type_mapping[dt] for col, dt in zip(column_names, column_dtypes)}

    try:
        raw_data = pd.read_fwf(data_file_path, widths=column_widths, names=column_names, dtype=dtypes)
        logger.info(f"Data file {data_file_path} parsed successfully.")
        return raw_data
    except Exception as e:
        logger.error(f"Error parsing data file {data_file_path}: {e}")
        raise


def write_ndjson(data_df: pd.DataFrame, output_file_path: str):
    """
    Write the parsed data to an NDJSON file.
    """
    try:
        with open(output_file_path, 'w') as f:
            for record in data_df.to_dict(orient='records'):
                json.dump(record, f)
                f.write('\n')
        logger.info(f"NDJSON file written: {output_file_path}")
    except Exception as e:
        logger.error(f"Error writing NDJSON file {output_file_path}: {e}")
        raise


def process_file_pair(data_file: str, specs_folder: str, output_folder: str):
    """
    Process a single data file and its corresponding specification file.
    """
    file_prefix = data_file.split('_')[0]
    spec_file_path = os.path.join(specs_folder, f"{file_prefix}.csv")
    data_file_path = os.path.join(DATA_FOLDER, data_file)
    output_file_path = os.path.join(output_folder, os.path.splitext(data_file)[0] + '.ndjson')

    if not os.path.exists(spec_file_path):
        logger.warning(f"Specification file for {data_file} not found. Skipping.")
        return

    try:
        spec_df = load_spec_file(spec_file_path)
        data_df = parse_data_file(data_file_path, spec_df)
        write_ndjson(data_df, output_file_path)
    except Exception as e:
        logger.error(f"Failed to process file pair {data_file} with spec {spec_file_path}: {e}")


def main(data_folder: str, specs_folder: str, output_folder: str):
    """
    Main entry point for processing all data and specification files.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logger.info(f"Created output directory: {output_folder}")

    for data_file in os.listdir(data_folder):
        if data_file.endswith('.txt'):
            logger.info(f"Processing data file: {data_file}")
            process_file_pair(data_file, specs_folder, output_folder)
        else:
            logger.info(f"Skipping non-data file: {data_file}")


if __name__ == "__main__":
    main(DATA_FOLDER, SPECS_FOLDER, OUTPUT_FOLDER)
