import datetime
import pandas as pd
from sklearn.impute import SimpleImputer
from lightgbm import LGBMClassifier
from snowflake.ml.model import model_signature
from sklearn.metrics import roc_auc_score


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
  # ftr_binary_classification.sqlのスキーマに合わせて特徴量を選択
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
      materialized="model",
      python_version="3.11",
      packages=["snowflake-ml-python", "pandas", "scikit-learn", "lightgbm"],
  )

  # ftr_binary_classificationテーブルを参照
  dataset = dbt.ref("ftr_binary_classification")
  data = dataset.to_pandas()

  x = preprocess(data)
  y = data["HAS_FREE_ORDER"]  # 二値分類ターゲット

  imputer = SimpleImputer()
  x = imputer.fit_transform(x)



  model = LGBMClassifier()
  model.fit(x, y)
  # AUC計算
  y_pred_proba = model.predict_proba(x)[:, 1]
  auc = roc_auc_score(y, y_pred_proba)

  return {
    "model": model,
    "signatures": {"predict": model_signature.infer_signature(x, y)},
    "version_name": datetime.datetime.today().strftime("V%Y%m%d%H%M%S"),
    "metrics": {"auc": auc},
    "comment": f"auc: {auc}",
    "set_default": True,
  }