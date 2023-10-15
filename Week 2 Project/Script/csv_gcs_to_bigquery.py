import argparse
import logging
import apache_beam as beam
from apache_beam.options import pipeline_options

class RowTransformer(beam.DoFn):
    
    def __init__(self, delimiter):
        self.delimiter = delimiter
        self._log = logging.getLogger('apache_beam.transforms.usercodetransformcontext')

    def process(self, row):
        import csv
        from datetime import datetime

        try:
            # Create a CSV reader object
            csv_object = csv.reader([row], delimiter=',', quotechar='"')
            values = next(csv_object)

            # Handle different 'Aired Date' formats
            if "-" in values[1]:  # Check if date range
                aired_date = datetime.strptime(values[1].split("-")[0].strip(), '%b %d, %Y').strftime('%Y-%m-%d')
            else:
                aired_date = datetime.strptime(values[1], '%b %d, %Y').strftime('%Y-%m-%d')

            self._log.info(f"Used Format for {values[1]} -> {aired_date}")

            return [{
                'name': values[0],
                'aired_date': aired_date,
                'year_of_release': int(values[2]),
                'original_network': values[3],
                'aired_on': values[4],
                'number_of_episodes': int(values[5]),
                'duration': values[6],
                'content_rating': values[7],
                'rating': float(values[8]),
                'synopsis': values[9],
                'genre': values[10],
                'tags': values[11],
                'director': values[12],
                'screenwriter': values[13],
                'cast': values[14],
                'production_companies': values[15],
                'rank': values[16]
            }]
        except Exception as e:
            self._log.error(f"Error processing row {row}. Error: {str(e)}")
            return []

#cleansing process tp merge every kdrama that split to a row  
class MergeLines(beam.DoFn):
    def __init__(self, delimiter=','):
        self.buffered_line = ''
        self.delimiter = delimiter
        self.num_columns = 17 
        self._log = logging.getLogger('apache_beam.transforms.usercodetransformcontext')

    def process(self, line):
        import csv

        # Continue appending lines to the buffer without adding extra spaces or stripping
        self.buffered_line += line

        # Attempt to parse the buffered data
        reader = csv.reader([self.buffered_line], delimiter=self.delimiter, quotechar='"')
        row = list(reader)[0]
        
        if len(row) == self.num_columns:
            result_line = self.buffered_line
            self.buffered_line = ''  # Clear the buffer for the next set of lines
            self._log.info(f"Merged Line: {result_line}")
            yield result_line


class LogData(beam.DoFn):
    def process(self, element):
        logging.info(element)
        yield element


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', dest='input', required=True, help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')
    parser.add_argument('--output', dest='output', required=True, help='Output BQ table to write results to.')
    parser.add_argument('--temp_location', dest='gcs_temp_location', required=True, help='GCS Temp Directory to store Temp data before write to BQ')                      
    parser.add_argument('--staging_location', dest='gcs_stg_location', required=True, help='GCS Staging Directory to store staging data before write to BQ')                      
    parser.add_argument('--project', dest='project_id', required=True, help='Project ID which GCP linked on.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    p_options = pipeline_options.PipelineOptions(
                pipeline_args, 
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location,
                project=known_args.project_id)

    bq_table_schema = {
        "fields": [
            {"name": "name", "type": "STRING"},
            {"name": "aired_date", "type": "DATE"},
            {"name": "year_of_release", "type": "INTEGER"},
            {"name": "original_network", "type": "STRING"},
            {"name": "aired_on", "type": "STRING"},
            {"name": "number_of_episodes", "type": "INTEGER"},
            {"name": "duration", "type": "STRING"},
            {"name": "content_rating", "type": "STRING"},
            {"name": "rating", "type": "FLOAT"},
            {"name": "synopsis", "type": "STRING"},
            {"name": "genre", "type": "STRING"},
            {"name": "tags", "type": "STRING"},
            {"name": "director", "type": "STRING"},
            {"name": "screenwriter", "type": "STRING"},
            {"name": "cast", "type": "STRING"},
            {"name": "production_companies", "type": "STRING"},
            {"name": "rank", "type": "STRING"}
        ]
    }

    with beam.Pipeline(options=p_options) as pipeline:
        output = ( 
            pipeline 
            | "Read CSV file from GCS" >> beam.io.ReadFromText(file_pattern=known_args.input, skip_header_lines=1)
            | "Log Raw Data" >> beam.ParDo(LogData())

            | "Merge Lines" >> beam.ParDo(MergeLines())
            | "Log Merged Lines" >> beam.ParDo(LogData())

            | "Transform Data" >> beam.ParDo(RowTransformer(','))
            | "Log transform Lines" >> beam.ParDo(LogData())

            | "Write data to BigQuery" >> beam.io.WriteToBigQuery(
                table=f'{known_args.project_id}:{known_args.output}',
                schema=bq_table_schema,
                custom_gcs_temp_location=known_args.gcs_temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                # additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
            )
        )

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
