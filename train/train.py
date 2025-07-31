import torch
from torch.utils.data import DataLoader
from torch.nn import BCELoss
from torch.optim import Adam
from tqdm import tqdm

from dataset import ZigzagDataset
from model import ZigzagCNNClassifier

# --- Configuration ---
DB_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
BATCH_SIZE = 32
EPOCHS = 10
LEARNING_RATE = 1e-3
MAX_CONTEXT_LEN = 50
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# --- Dataset and Loader ---
dataset = ZigzagDataset(db_url=DB_URL, max_context_len=MAX_CONTEXT_LEN)
loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

# --- Model ---
model = ZigzagCNNClassifier(
    metadata_dim=6,
    context_vocab_size=len(dataset.vocab),
    context_embed_dim=8,
    context_len=MAX_CONTEXT_LEN
).to(DEVICE)

# --- Optimizer and Loss ---
optimizer = Adam(model.parameters(), lr=LEARNING_RATE)
criterion = BCELoss()

# --- Training Loop ---
for epoch in range(EPOCHS):
    model.train()
    epoch_loss = 0
    correct = 0
    total = 0

    for tick_window, zigzag_context, metadata, label in tqdm(loader, desc=f"Epoch {epoch+1}"):
        tick_window = tick_window.to(DEVICE)
        zigzag_context = zigzag_context.to(DEVICE)
        metadata = metadata.to(DEVICE)
        label = label.to(DEVICE)

        optimizer.zero_grad()
        output = model(tick_window, zigzag_context, metadata)

        loss = criterion(output, label)
        loss.backward()
        optimizer.step()

        epoch_loss += loss.item()

        preds = (output > 0.5).float()
        correct += (preds == label).sum().item()
        total += label.size(0)

    acc = correct / total
    print(f"✅ Epoch {epoch+1}: Loss = {epoch_loss:.4f} | Accuracy = {acc:.4f}")

# --- Save Model ---
torch.save(model.state_dict(), "zigzag_model.pt")
print("✅ Model saved as zigzag_model.pt")
