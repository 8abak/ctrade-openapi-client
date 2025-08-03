import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
import torch.optim as optim
import numpy as np
import psycopg2
import ast

DB_CONFIG = {
    'dbname': 'trading',
    'user': 'babak',
    'password': 'babak33044',
    'host': 'localhost',
    'port': '5432'
}

# Simple model
class ZigzagNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(500 + 10, 128)
        self.fc2 = nn.Linear(128, 64)
        self.fc3 = nn.Linear(64, 1)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        return torch.sigmoid(self.fc3(x))

# Custom dataset
class ZigzagDataset(Dataset):
    def __init__(self):
        self.records = []
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tick_window, atr140, atr15, stddev_before, stddev_after, slope, time_of_day, label FROM zigzag_training_data")
                for row in cur.fetchall():
                    tick_window = np.array(ast.literal_eval(row[0]))
                    meta = np.array(row[1:7], dtype=np.float32)
                    input_vec = np.concatenate((tick_window, meta))
                    label = float(row[7])
                    self.records.append((input_vec, label))

    def __len__(self):
        return len(self.records)

    def __getitem__(self, idx):
        x, y = self.records[idx]
        return torch.tensor(x, dtype=torch.float32), torch.tensor(y, dtype=torch.float32)


def train_model():
    dataset = ZigzagDataset()
    loader = DataLoader(dataset, batch_size=32, shuffle=True)
    model = ZigzagNet()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    loss_fn = nn.BCELoss()

    for epoch in range(10):
        total_loss = 0
        for x, y in loader:
            y = y.view(-1, 1)  # reshape target
            pred = model(x)
            loss = loss_fn(pred, y)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            total_loss += loss.item()

        print(f"Epoch {epoch+1}, Loss: {total_loss:.4f}")

    torch.save(model.state_dict(), "cTrader/ml/model_weights.pt")
    print("✅ Model trained and saved.")

if __name__ == '__main__':
    train_model()
