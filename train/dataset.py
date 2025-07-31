import torch
from torch.utils.data import Dataset
import numpy as np
import psycopg2
import json
from sqlalchemy import create_engine
import pandas as pd

class ZigzagDataset(Dataset):
    def __init__(self, db_url, vocab=None, max_context_len=50):
        self.engine = create_engine(db_url)
        self.df = pd.read_sql("SELECT * FROM zigzag_training_data", self.engine)

        self.vocab = vocab or self.build_vocab(self.df["zigzag_context"])
        self.max_context_len = max_context_len

    def build_vocab(self, context_series):
        """Build a simple vocabulary from unique direction:level tokens."""
        tokens = set()
        for row in context_series:
            for token in row:
                tokens.add(token)
        vocab = {tok: i+1 for i, tok in enumerate(sorted(tokens))}  # 0 is reserved for padding
        vocab["<PAD>"] = 0
        return vocab

    def encode_context(self, context_tokens):
        """Encode string tokens into integer IDs using vocab."""
        ids = [self.vocab.get(tok, 0) for tok in context_tokens]
        if len(ids) < self.max_context_len:
            ids += [0] * (self.max_context_len - len(ids))
        else:
            ids = ids[:self.max_context_len]
        return torch.tensor(ids, dtype=torch.long)

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]

        tick_window = torch.tensor(row["tick_window"], dtype=torch.float32)
        zigzag_context = self.encode_context(row["zigzag_context"])
        metadata = torch.tensor([
            row["atr140"], row["atr15"], row["stddev_before"],
            row["stddev_after"], row["slope"], row["time_of_day"]
        ], dtype=torch.float32)
        label = torch.tensor(row["label"], dtype=torch.float32)

        return tick_window, zigzag_context, metadata, label
