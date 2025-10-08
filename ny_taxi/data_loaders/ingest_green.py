import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    years  = list(range(2025,2026))
    months = list(range(1,13))
    rows_green = []


    for y in years:
        for m in months:
            url_green = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{y}-{m:02d}.parquet"
            
            rows_green.append({'year': y, 'month': m, 'url': url_green})
    return pd.DataFrame(rows_green)
@test
def test_output(output, *args) -> None:
    assert output is not None and len(output) > 0
