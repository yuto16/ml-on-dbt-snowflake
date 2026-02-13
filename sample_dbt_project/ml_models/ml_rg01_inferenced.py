import pandas as pd
from sklearn.impute import SimpleImputer
from snowflake.ml.registry import registry

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    features = [
        "ORDER_COUNT",
        "TOTAL_SPENT",
        "AVG_ORDER_VALUE"
    ]
    return df[features]

def model(dbt, session):
    dbt.config(
        materialized="table",
        python_version="3.11",
        packages=["snowflake-ml-python", "pandas", "scikit-learn", "lightgbm"],
    )
    dataset = dbt.ref("ftr_regression")
    data = dataset.to_pandas()
    x = preprocess(data)
    imputer = SimpleImputer()
    x = imputer.fit_transform(x)
    reg = registry.Registry(session=session)
    model_ref = dbt.ref("ml_rg01_model")
    mv = reg.get_model(model_ref.table_name).default
    y_pred = mv.run(x, function_name="predict")
    result = data[["CUSTOMER_ID"]].copy()
    result["PREDICTED_LIFETIME_VALUE"] = y_pred
    return result
