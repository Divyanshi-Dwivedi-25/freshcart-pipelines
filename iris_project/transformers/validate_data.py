
from mage_ai.data_preparation.decorators import transformer, test

@transformer
def validate_data(df, *args, **kwargs):

    assert df.isnull().sum().sum() == 0, "Missing values found!"

    assert df['target'].nunique() == 3, "Expected exactly 3 classes!"

    print("Data Validation Successful!")
    print("Missing values:", df.isnull().sum().sum())
    print("Number of classes:", df['target'].nunique())

    return df


@test
def test_no_missing_values(output, *args):
    assert output.isnull().sum().sum() == 0


@test
def test_three_classes(output, *args):
    assert output['target'].nunique() == 3
