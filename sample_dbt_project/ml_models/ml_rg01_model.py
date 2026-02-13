import datetime
import pandas as pd
from sklearn.impute import SimpleImputer
from lightgbm import LGBMRegressor
from snowflake.ml.model import model_signature
from sklearn.metrics import r2_score

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    features = [
        "ORDER_COUNT",
        "TOTAL_SPENT",
        "AVG_ORDER_VALUE"
    ]
    return df[features]

def model(dbt, session):
    dbt.config(
        materialized="model",
        python_version="3.11",
        packages=["snowflake-ml-python", "pandas", "scikit-learn", "lightgbm"],
    )
    dataset = dbt.ref("ftr_regression")
    data = dataset.to_pandas()
    x = preprocess(data)
    y = data["TARGET_LIFETIME_VALUE"]  # 回帰ターゲット
    imputer = SimpleImputer()
    x = imputer.fit_transform(x)
    model = LGBMRegressor()
    model.fit(x, y)
    y_pred = model.predict(x)
    r2 = r2_score(y, y_pred)
    return {
        "model": model,
        "signatures": {"predict": model_signature.infer_signature(x, y)},
        "version_name": datetime.datetime.today().strftime("V%Y%m%d%H%M%S"),
        "metrics": {"r2": r2},
        "comment": f"r2: {r2}",
        "set_default": True,
    }
