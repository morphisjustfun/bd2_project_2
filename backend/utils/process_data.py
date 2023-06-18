import json

input_file = '/Users/mariojacoboriosgamboa/Lordmarcusvane/Hackprog/universidad/BD2/bd2_project_2/backend/data/data.json'
output_file = '/Users/mariojacoboriosgamboa/Lordmarcusvane/Hackprog/universidad/BD2/bd2_project_2/backend/data/processed_data.json'

with open(input_file, 'r') as file:
    with open(output_file, 'w') as outfile:
        for line in file:
            data = json.loads(line)
            # conserve only id, submitter, title, doi and abstract
            data = {k: v for k, v in data.items() if k in ['id', 'submitter', 'title', 'doi', 'abstract']}
            # if any of the fields is empty, make it ""
            data = {k: v if v else "" for k, v in data.items()}
            # remove \ from fields
            escaped_datadata = {k: v.replace('\\', '') for k, v in data.items()}
            escaped_datadata = {k: v.replace('\n', '\\n') for k, v in escaped_datadata.items()}
            escaped_datadata = {k: v.replace('"', '') for k, v in escaped_datadata.items()}
            escaped_datadata = {k: v.replace("'", '') for k, v in escaped_datadata.items()}
            outfile.write(json.dumps(escaped_datadata) + '\n')
