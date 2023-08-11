import dlt
from dlt.sources.helpers import requests

from google.cloud import bigquery
from google.oauth2 import service_account


@dlt.source
def bigquery_source(api_secret_key=dlt.secrets.value):
    return bigquery_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers


@dlt.resource(write_disposition="append")
def bigquery_resource():

    service_account_info = {
        "project_id": dlt.secrets.get('source.bigquery.credentials.project_id'),
        "private_key": dlt.secrets.get('source.bigquery.credentials.private_key'),
        "client_email": dlt.secrets.get('source.bigquery.credentials.client_email'),
        "token_uri": dlt.secrets.get('source.bigquery.credentials.token_uri'),
        "location": dlt.secrets.get('source.bigquery.credentials.location'),
    }
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials)


    query_str = f"""
        select * from `dlthub-analytics.analytics_345886522.events_20230808`
    """

    print('loading rows')
    for row in client.query(query_str):
        yield {key:value for key,value in row.items()}


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='bigquery', destination='motherduck', dataset_name='bigquery_data'
    )

    # print credentials by running the resource
    data = list(bigquery_resource())

    # print the data yielded from resource
    # print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(data, table_name="ga_events")

    # pretty print the information on data that was loaded
    print(load_info)

    pipeline = dlt.pipeline(
        pipeline_name="bigquery",
        destination="motherduck",
        dataset_name="transformed_data"
    )

    venv = dlt.dbt.get_venv(pipeline)

    dbt = dlt.dbt.package(
        pipeline,
        "dbt_bigquery",
        venv=venv
    )

    models = dbt.run_all()

    for m in models:
        print(
            f"Model {m.model_name} materialized" +
            f"in {m.time}" +
            f"with status {m.status}" +
            f"and message {m.message}"
        )