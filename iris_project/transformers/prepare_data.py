import pandas as pd
from sklearn.model_selection import train_test_split


def transform(df):
    train_df, test_df = train_test_split(
        df,
        test_size=0.3,
        random_state=42,
        stratify=df["target"]
    )

    train_df = train_df.copy()
    test_df = test_df.copy()

    train_df["split"] = "train"
    test_df["split"] = "test"

    result = pd.concat([train_df, test_df])

    return result