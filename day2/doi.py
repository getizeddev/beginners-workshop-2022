import dlt
import datetime
import requests


@dlt.source
def doi(doi_url, year):
    return (
        providers(doi_url, year),
        clients(doi_url, year)
    )


@dlt.resource(write_disposition="replace")
def providers(doi_url, year):
    """Yields player profiles for a list of player usernames"""
    for num in year:
        r = requests.get(f"{doi_url}/providers", params={"year":num})
        r.raise_for_status()
        status = r.json()["data"]
        for x in status:
            yield x


@dlt.resource(write_disposition="replace")
def clients(doi_url, year):
    """Yields url to game archives for specified players."""
    for num in year:
        r = requests.get(f"{doi_url}/clients", params={"year":num})
        r.raise_for_status()
        status = r.json()["data"]
        for x in status:
            yield x
        

if __name__ == "__main__" :

    # configure the pipeline: provide the destination and dataset name to which the data should go
    p = dlt.pipeline(destination='bigquery', dataset_name='doi_data', full_refresh=False)

    # load the data from the chess source
    info = p.run(
        doi(
            "https://api.test.datacite.org",
            (2021, 2022)
      )
    )
    print(info)
