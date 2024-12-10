import os
import pandas as pd
import json

data_folder = 'data'
specs_folder = 'specs'
output_folder = 'output'

#Dict for mapping spec datatypes to pandas datatypes
type_mapping = {
    'TEXT': 'object',
    'BOOLEAN': 'bool',
    'INTEGER': 'int64',
    'FLOAT': 'float64',
}

for data_file in os.listdir(data_folder):
    if data_file.endswith('.txt'):
        try:
            file_prefix = data_file.split('_')[0]
            spec_file = os.path.join(specs_folder, f"{file_prefix}.csv")

            # Check if the spec file exists
            if not os.path.exists(spec_file):
                print(f"Error: Specification file {spec_file} not found for {data_file}. Skipping.")
                continue

            try:
                spec_df = pd.read_csv(spec_file, sep=',')
            except Exception as e:
                print(f"Error reading specification file {spec_file}: {e}")
                continue

            column_names = spec_df['column name'].tolist()
            column_widths = spec_df['width'].tolist()
            column_dtypes = spec_df['datatype'].tolist()

            dtypes = {col: type_mapping[dt] for col, dt in zip(column_names, column_dtypes)}

            data_file_path = os.path.join(data_folder, data_file)
            try:
                data_df = pd.read_fwf(data_file_path, widths=column_widths, names=column_names, dtype=dtypes)
            except Exception as e:
                print(f"Error reading data file {data_file_path}: {e}")
                continue

            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            output_file_path = os.path.join(output_folder, data_file.split('.')[0]) + '.ndjson'

            ndjson_lines = data_df.to_dict(orient='records')
            with open(output_file_path, 'w') as f:
                for line in ndjson_lines:
                    f.write(json.dumps(line) + '\n')
            print(f"Output file written for {data_file.split('.')[0]}.")

        except Exception as e:
            print(f'ERROR: while processing {data_file}: {e}')
    else:
        print(f"Skipping non-.txt file {data_file}")
