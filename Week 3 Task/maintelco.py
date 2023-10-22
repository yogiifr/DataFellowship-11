import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project', required=True, default='handy-honor-401601',
                        help='Google Cloud Project ID')
    parser.add_argument('--bucket', dest='bucket', required=True, default='gs://fellowship-yogi/telcoTask/',
                        help='Google Cloud Storage Bucket')
    parser.add_argument('--input_file', dest='input_file', required=True, default='gs://fellowship-yogi/telcoTask/csvs/WA_Fn-UseC_-Telco-Customer-Churn.csv',
                        help='Input CSV file in GCS')
    parser.add_argument('--dataset', dest='dataset', required=True, default='iykra11',
                        help='BigQuery Dataset ID')
    parser.add_argument('--table', dest='table', required=True, default='telco-task-main',
                        help='BigQuery Table ID')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the CSV data from GCS
        csv_data = (p
                    | "Read CSV from GCS" >> beam.io.ReadFromText(f'gs://{known_args.bucket}/{known_args.input_file}')
                    | "Parse CSV" >> beam.Map(lambda line: line.split(',')))

        # Transform the data as needed (modify this part to suit your needs)
        transformed_data = (csv_data
                            | "Transform Data" >> beam.Map(lambda fields: {
                                'customerID': fields[0],
                                'gender': fields[1],
                                'SeniorCitizen': int(fields[2]),
                                'Partner': fields[3],
                                'Dependents': fields[4],
                                'tenure': int(fields[5]),
                                'PhoneService': fields[6],
                                'MultipleLines': fields[7],
                                'InternetService': fields[8],
                                'OnlineSecurity': fields[9],
                                'OnlineBackup': fields[10],
                                'DeviceProtection': fields[11],
                                'TechSupport': fields[12],
                                'StreamingTV': fields[13],
                                'StreamingMovies': fields[14],
                                'Contract': fields[15],
                                'PaperlessBilling': fields[16],
                                'PaymentMethod': fields[17],
                                'MonthlyCharges': float(fields[18]),
                                'TotalCharges': fields[19],
                                'Churn': fields[20]
                            }))

        # Write the transformed data to BigQuery
        (transformed_data
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            known_args.table,
            dataset=known_args.dataset,
            project=known_args.project,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         ))

if __name__ == '__main__':
    run()
