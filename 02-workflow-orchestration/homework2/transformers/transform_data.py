if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    print(f"Before filter passenger_count > 0 and trip_distance > 0: {len(data)}")
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    print(f"After filter passenger_count > 0 and trip_distance > 0: {len(data)}")

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.rename({'VendorID': 'vendor_id',
        'RatecodeID': 'rate_code_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',},
        axis=1, inplace=True
    )
    
    # What are the existing values of VendorID in the dataset?
    unique_vendor_id_list = list(data['vendor_id'].unique())
    print(f"Existing values of VendorID in the dataset: {unique_vendor_id_list}")


    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # vendor_id is one of the existing values in the column (currently)
    assert 'vendor_id' in output.columns, "'vendor_id' is not one of the existing values in the column"

    # passenger_count is greater than 0
    assert (output['passenger_count'] > 0).all(), 'There are rows with passenger count equal to 0'

    # trip_distance is greater than 0
    assert (output['trip_distance'] > 0).all(), 'There are rows with trip distance equal to 0'
