import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging

class ParseCSVDoFn(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO

        reader = csv.reader(StringIO(element))
        for row in reader:
            try:
                if len(row) != 7:
                    continue
                yield {
                    'restaurant_id': int(row[0]),
                    'location_id': int(row[1]),
                    'cuisine_id': int(row[2]),
                    'rate': float(row[3]),
                    'votes': int(row[4]),
                    'approx_cost_for_two': int(row[5]),
                    'ingestion_date': row[6],
                }
            except Exception as e:
                logging.error(f"Failed to parse row {row}: {e}")

class ExtractGCSPathDoFn(beam.DoFn):
    def process(self, element):
        message = json.loads(element.decode('utf-8'))
        yield message['gcs_path']

def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic')
    parser.add_argument('--output_table')
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        gcs_paths = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            | "Extract GCS Path" >> beam.ParDo(ExtractGCSPathDoFn())
        )

        (
            gcs_paths
            | "Read CSV files from GCS" >> beam.io.ReadAllFromText()
            | "Parse CSV lines" >> beam.ParDo(ParseCSVDoFn())
            | "Write to BigQuery (Staging)" >> beam.io.WriteToBigQuery(
                known_args.output_table,  
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                schema={
                    "fields": [
                        {"name": "restaurant_id", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "location_id", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "cuisine_id", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "rate", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "votes", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "approx_cost_for_two", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
                    ]
                }
            )
        )

if __name__ == '__main__':
    run()