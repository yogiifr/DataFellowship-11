import apache_beam as beam
import argparse
from apache_beam.options import pipeline_options
import logging

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--project', dest='project_id', required=True,
                      help='Project ID corresponding to the GCP owner')
  parser.add_argument('--input_table', dest='input', required=True,
                      help='BigQuery table source to Read')
  parser.add_argument('--output', dest='output', required=True,
                      help='BigQuery table target to Write')
  parser.add_argument('--temp_location', dest='gcs_temp_location', required=True,
                      help='GCS Temp Directory to store Temp data before write to BQ')
  parser.add_argument('--staging_location', dest='gcs_stg_location', required=True,
                      help='GCS Staging Directory to store staging data before write to BQ')

  known_args, pipeline_args = parser.parse_known_args(argv)
  p_options = pipeline_options.PipelineOptions(
                pipeline_args,
                project=known_args.project_id,
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location
              )
  
  with beam.Pipeline(options=p_options) as pipeline:
    bq_table_schema = {
            'fields': [
                {'name': 'customerID', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SeniorCitizen', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'Partner', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}, 
                {'name': 'Dependents', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'tenure', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'PhoneService', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'MultipleLines', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'InternetService', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'OnlineSecurity', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'OnlineBackup', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'DeviceProtection', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'TechSupport', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'StreamingTV', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'StreamingMovies', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'Contract', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'PaperlessBilling', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'PaymentMethod', 'type': 'STRING', 'mode': 'NULLABLE'}, 
                {'name': 'MonthlyCharges', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'TotalCharges', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'Churn', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
            ]
    }
    
    output = ( pipeline 
                | "Read data from BigQuery" >> beam.io.ReadFromBigQuery(
                                  query='''SELECT
                                              gender,
                                              SeniorCitizen,
                                              Partner,
                                              Dependents,
                                              AVG(tenure) AS AvgTenure,
                                              MAX(MonthlyCharges) AS MaxMonthlyCharges,
                                              MIN(MonthlyCharges) AS MinMonthlyCharges,
                                              AVG(MonthlyCharges) AS AvgMonthlyCharges,
                                              MAX(TotalCharges) AS MaxTotalCharges,
                                              MIN(TotalCharges) AS MinTotalCharges
                                          FROM
                                              talcoTask-backup
                                          GROUP BY
                                              gender, SeniorCitizen, Partner, Dependents;'''
                                  ,
                                  use_standard_sql=True)
                | "Write data to BigQuery" >> beam.io.WriteToBigQuery(
                      table=f'{known_args.project_id}:{known_args.output}',
                      schema=bq_table_schema,
                      custom_gcs_temp_location=known_args.gcs_temp_location,
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                  )
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()