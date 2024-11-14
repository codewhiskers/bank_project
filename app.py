from flask import Flask, render_template, jsonify
import pandas as pd
import pdb
import json

app = Flask(__name__)

# Create a sample pandas DataFrame
df_hq = pd.read_csv('data/test_data/df_hq.csv')    
# df_hq = df_hq[0:500]
df_hq.fillna('None', inplace=True)
columns_to_keep = [
    'rssd_id',
    # 'ncua_id',
    # 'fdic_id',
    # 'occ_id',
    'entity_type',
    'entity_name',
    'city',
    'state',
    
]
df_hq = df_hq[columns_to_keep]
# f = open('data/test_data/data_dictionary.json')
# data_dictionary = json.load(f)
# pdb.set_trace()
df_hq2 = df_hq[['rssd_id', 'state']]

@app.route('/')
def index():
    # Render the HTML template
    return render_template('index.html')

@app.route('/data1')
def data1():
    # Convert DataFrame to list of records
    data = df_hq.to_dict(orient="records")
    # Get column names dynamically
    columns = [{"data": col, "title": col} for col in df_hq.columns]
    return jsonify({"data": data, "columns": columns})

@app.route('/data2')
def data2():
    # Convert DataFrame to list of records
    data = df_hq2.to_dict(orient="records")
    # Get column names dynamically
    columns = [{"data": col, "title": col} for col in df_hq2.columns]
    return jsonify({"data": data, "columns": columns})

if __name__ == '__main__':
    app.run(debug=True)
