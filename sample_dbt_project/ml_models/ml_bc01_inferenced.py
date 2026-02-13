import pandas as pd
from sklearn.impute import SimpleImputer
from snowflake.ml.registry import registry


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
  features = [
    "ORDER_COUNT",
    "TOTAL_SPENT",
    "PAID_ORDERS",
    "FREE_ORDERS",
    "TOTAL_ORDERS"
  ]
  return df[features]


def model(dbt, session):
    dbt.config(
        materialized="table",
        python_version="3.11",
        packages=["snowflake-ml-python", "pandas", "scikit-learn", "lightgbm"],
    )

    dataset = dbt.ref("ftr_binary_classification")
    data = dataset.to_pandas()
    x = preprocess(data)
    imputer = SimpleImputer()
    x = imputer.fit_transform(x)

    reg = registry.Registry(session=session)
    model_ref = dbt.ref("ml_bc01_model")
    mv = reg.get_model(model_ref.table_name).default
    # LightGBMのpredictは0/1を返す
    y_pred = mv.run(x, function_name="predict")
    result = data[["CUSTOMER_ID"]].copy()
    result["PREDICTED_HAS_FREE_ORDER"] = y_pred
    return result