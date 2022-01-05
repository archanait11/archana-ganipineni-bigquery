import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

dest_schema = {
    'fields': [
        {'name': 'first_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'REQUIRED'}
    ]
}

dest_table = bigquery.TableReference(
    projectId="york-cdf-start",
    datasetId="aganipineni_proj_1",
    tableId="usd_customers"
)


def run():
    pipeline_options = PipelineOptions(
        temp_location='gs://archana-python-bucket/temp',
        project='york-cdf-start',
        region='us-central1',
        staging_location='gs://archana-python-bucket/staging',
        job_name='archana-ganipineni-bq1',
        save_main_session=True
    )
    with beam.Pipeline(runner='DataflowRunner', options=pipeline_options) as p:
        data_from_usd = p | "Read data from usd" >> beam.io.ReadFromBigQuery(
            query="select first_name,last_name "
                  "FROM york-cdf-start.aganipineni_proj_1.usd_order_payment_history LIMIT 7", project="york-cdf-start",
            use_standard_sql=True)  # | "print" >> beam.Map(print)
        # write to new table
        data_from_usd | beam.io.WriteToBigQuery(
            dest_table,
            schema=dest_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('reading and writing to BigQuery')
    run()
