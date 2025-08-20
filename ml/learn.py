# ml/learn.py
from typing import Dict, List
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
import joblib
import os

class OnlineMoveModel:
    """
    One model per threshold_usd (2,3,4,5). 3-class: 'up','dn','nt'
    """
    def __init__(self, threshold_usd: int, model_dir: str = "models"):
        self.threshold = int(threshold_usd)
        self.model_dir = model_dir
        self.classes_ = np.array(['dn','nt','up'])
        os.makedirs(self.model_dir, exist_ok=True)
        self.path = os.path.join(self.model_dir, f"move_{self.threshold}.joblib")
        self.model = None
        self._load()

    def _new(self):
        # pipeline: Standardize -> SGD (logistic)
        return make_pipeline(
            StandardScaler(with_mean=False),
            SGDClassifier(loss='log', penalty='l2', alpha=1e-5,
                          learning_rate='optimal', max_iter=1, tol=None)
        )

    def _load(self):
        if os.path.exists(self.path):
            self.model = joblib.load(self.path)
        else:
            self.model = self._new()

    def save(self):
        joblib.dump(self.model, self.path)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict_proba(X)  # columns align with classes_

    def partial_fit(self, X: pd.DataFrame, y: List[str]):
        self.model.named_steps['sgdclassifier'].partial_fit(
            X, np.array(y), classes=self.classes_
        )
