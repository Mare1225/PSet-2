import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    years  = list(range(2025,2026))
    months = list(range(1,13))
    rows_yellow = []

    for y in years:
        for m in months:
            url_yellow = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m:02d}.parquet"
            rows_yellow.append({'year': y, 'month': m, 'url': url_yellow})
            
    return pd.DataFrame(rows_yellow)
@test
def test_output(output, *args) -> None:
    assert output is not None and len(output) > 0
