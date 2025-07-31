import torch
import torch.nn as nn

class ZigzagCNNClassifier(nn.Module):
    def __init__(self, 
                 tick_window_len=500, 
                 metadata_dim=6, 
                 context_vocab_size=200, 
                 context_embed_dim=8, 
                 context_len=50):
        super().__init__()

        # 1D CNN for tick window (e.g., 500 mid deltas)
        self.tick_conv = nn.Sequential(
            nn.Conv1d(in_channels=1, out_channels=32, kernel_size=7, padding=3),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=2),   # output: [B, 32, 250]
            nn.Conv1d(32, 64, kernel_size=5, padding=2),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1)        # output: [B, 64, 1]
        )

        # Embedding + CNN for zigzag context (encoded as integers)
        self.zigzag_embedding = nn.Embedding(
            num_embeddings=context_vocab_size,
            embedding_dim=context_embed_dim,
            padding_idx=0
        )
        self.context_conv = nn.Sequential(
            nn.Conv1d(context_embed_dim, 32, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1)  # output: [B, 32, 1]
        )

        # Dense projection for metadata
        self.meta_dense = nn.Sequential(
            nn.Linear(metadata_dim, 32),
            nn.ReLU()
        )

        # Final classifier
        self.classifier = nn.Sequential(
            nn.Linear(64 + 32 + 32, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )

    def forward(self, tick_window, zigzag_context, metadata):
        """
        Inputs:
            tick_window: Tensor of shape [B, 500]
            zigzag_context: Tensor of shape [B, context_len] (integer-encoded)
            metadata: Tensor of shape [B, metadata_dim]
        Output:
            prob: Tensor of shape [B], probability of being zAbs3.0
        """
        # Tick window path
        x_tick = tick_window.unsqueeze(1)          # [B, 1, 500]
        x_tick = self.tick_conv(x_tick).squeeze(-1)  # [B, 64]

        # Zigzag context path
        x_zigzag = self.zigzag_embedding(zigzag_context)        # [B, L, D]
        x_zigzag = x_zigzag.transpose(1, 2)                     # [B, D, L]
        x_zigzag = self.context_conv(x_zigzag).squeeze(-1)      # [B, 32]

        # Metadata path
        x_meta = self.meta_dense(metadata)                      # [B, 32]

        # Combine and classify
        x = torch.cat([x_tick, x_zigzag, x_meta], dim=1)        # [B, 128]
        out = self.classifier(x).squeeze(-1)                    # [B]
        return out
