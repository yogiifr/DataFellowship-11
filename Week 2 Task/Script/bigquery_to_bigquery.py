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
      "fields": [
            {"name": "year_of_release", "type": "INTEGER"},
            {"name": "great", "type": "INTEGER"},
            {"name": "excellent", "type": "INTEGER"},
            {"name": "exceptional", "type": "INTEGER"},
            {"name": "avg_episodes", "type": "FLOAT"}
      ]
    }
    
    output = ( pipeline 
                | "Read data from BigQuery" >> beam.io.ReadFromBigQuery(
                                  query='''WITH categorized_ratings AS (
                                          SELECT
                                            year_of_release,
                                            CASE
                                              WHEN rating < 8.5 THEN 'Great'
                                              WHEN rating >= 8.5 AND rating < 9 THEN 'Excellent'
                                              WHEN rating >= 9 THEN 'Exceptional'
                                            END AS rating_category,
                                            number_of_episodes
                                          FROM fellowship11.raws_data
                                        ) ''' \

                                        '''SELECT
                                            year_of_release,
                                            COUNTIF(rating_category = 'Great') AS great,
                                            COUNTIF(rating_category = 'Excellent') AS excellent,
                                            COUNTIF(rating_category = 'Exceptional') AS exceptional,
                                            round(AVG(number_of_episodes),2) AS avg_episodes
                                          FROM
                                            categorized_ratings
                                          GROUP BY
                                            year_of_release
                                          ORDER BY
                                            year_of_release DESC'''
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