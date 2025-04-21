import json
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset, random_split
import numpy as np
from sklearn.metrics import precision_recall_curve

import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'TRUE'

# 修改后的模型定义
class SimpleClassifier(nn.Module):
    def __init__(self, input_dim):
        super(SimpleClassifier, self).__init__()
        self.block = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.Linear(128, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.1),
        )
        self.fc = nn.Linear(128, 1)
        # 初始化偏置增加正类倾向
        nn.init.constant_(self.fc.bias, 0.2)

    def forward(self, x):
        x = self.block(x)
        return self.fc(x)

# 数据集类保持不变
class VectorDataset(Dataset):
    def __init__(self, positive_embeddings, negative_embeddings, 
            positive_contents, negative_contents, positive_label=1, negative_label=0, dimension=512):
        self.data = []
        self.labels = []
        self.contents = []

        for embedding, content in zip(positive_embeddings, positive_contents):
            if len(embedding)==dimension:
                self.data.append(np.array(embedding, dtype=np.float32))
                self.labels.append(positive_label)
                self.contents.append(content)

        for embedding, content in zip(negative_embeddings, negative_contents):
            if len(embedding)==dimension:
                self.data.append(np.array(embedding, dtype=np.float32))
                self.labels.append(negative_label)
                self.contents.append(content)

        self.data = np.array(self.data)
        self.labels = np.array(self.labels, dtype=np.float32)
        self.contents = np.array(self.contents)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return (
            torch.tensor(self.data[idx]),
            torch.tensor(self.labels[idx]),
            self.contents[idx],
        )

def split_dataset(dataset, train_ratio=0.8):
    train_size = int(train_ratio * len(dataset))
    test_size = len(dataset) - train_size
    return random_split(dataset, [train_size, test_size])

# 修改后的训练函数
def train_model(model, train_loader, criterion, optimizer, device):
    model.train()
    running_loss = 0.0
    for inputs, labels, _ in train_loader:
        inputs, labels = inputs.to(device), labels.to(device).unsqueeze(1)
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
        running_loss += loss.item()
    return running_loss / len(train_loader)

# 修改后的测试函数（添加召回率计算）
def test_model_with_content(model, test_loader, criterion, device, threshold=0.5):
    model.eval()
    running_loss = 0.0
    all_outputs = []
    all_labels = []
    all_contents = []
    
    with torch.no_grad():
        for inputs, labels, contents in test_loader:
            inputs, labels = inputs.to(device), labels.to(device).unsqueeze(1)
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            running_loss += loss.item()

            all_outputs.append(outputs.cpu().numpy())
            all_labels.append(labels.cpu().numpy()>threshold)
            all_contents.extend(contents)
    
    # 计算召回率和精确率
    all_outputs = np.concatenate(all_outputs)
    all_labels = np.concatenate(all_labels)
    probas = torch.sigmoid(torch.tensor(all_outputs)).numpy()
    predictions = (probas > threshold).astype(int)
    
    # 计算召回率
    true_pos = np.sum((predictions == 1) & (all_labels == 1))
    actual_pos = np.sum(all_labels == 1)
    recall = true_pos / (actual_pos + 1e-8)
    
    return running_loss / len(test_loader), recall, probas, all_labels, all_contents

# 添加阈值自动调整函数
def find_optimal_threshold(probas, labels, target_recall=0.8):
    precision, recall, thresholds = precision_recall_curve(labels, probas)
    
    # 寻找满足最小召回率要求的阈值
    viable_thresholds = [t for t, r in zip(thresholds, recall) if r >= target_recall]
    if viable_thresholds:
        return min(viable_thresholds)
    else:
        return 0.5  # 默认阈值

if __name__ == "__main__":
    # import pandas as pd
    import matplotlib.pyplot as plt
    
    # 超参数设置
    input_dim = 512
    batch_size = 64
    learning_rate = 0.001
    num_epochs = 40
    negative_label=0.1
    # 由于负例中也可能存在有用的，正例中一定是有用的，因此给negative一个松弛
    positive_label=0.9
    train_threshold=0.8
    train_sample_bias=1.5
    # 在考虑样本数量的基础上，放大正例的比例
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    # 加载数据
    # positive_file = "DataSet\\with_supply_article_embed.json"
    # negative_file = "DataSet\\without_supply_article_embed.json"
    
    positive_file = "DataSet\ArticleDiscrimination\with_supply_data_list_new.json"
    negative_file = "DataSet\ArticleDiscrimination\without_supply_data_list_new.json"

    def load_data_from_json(positive_file, negative_file):
        "如果weight设置为None，则代表根据正负样本的比例自动确定权重"
        with open(positive_file, "r", encoding="utf-8") as f:
            positive_data = json.load(f)
        with open(negative_file, "r", encoding="utf-8") as f:
            negative_data = json.load(f)

        return (
            [item["article_embedding"] for item in positive_data],
            [item["article_embedding"] for item in negative_data],
            [item["title"] for item in positive_data],
            [item["title"] for item in negative_data],
        )

    positive_embeddings, negative_embeddings, positive_contents, negative_contents = load_data_from_json(positive_file, negative_file)

    # 创建数据集
    dataset = VectorDataset(
        positive_embeddings, negative_embeddings, 
        positive_contents, negative_contents,
        positive_label=positive_label, negative_label=negative_label
    )
    train_dataset, test_dataset = split_dataset(dataset)

    # 计算正样本权重
    num_pos = len(positive_embeddings)
    num_neg = len(negative_embeddings)
    
    pos_weight = torch.tensor([num_neg / num_pos * train_sample_bias]).to(device)
    # pos_weight = torch.tensor(3)

    # 创建数据加载器
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    # 初始化模型和优化器
    model = SimpleClassifier(input_dim).to(device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)  # 修改损失函数（这里设置了正样本的权重，由于负样本是正样本的二倍，所以这里是正样本权重较大）
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    # 训练循环
    best_recall = 0.0
    for epoch in range(num_epochs):
        train_loss = train_model(model, train_loader, criterion, optimizer, device)
        test_loss, test_recall, probas, labels, contents = test_model_with_content(
            model, test_loader, criterion, device, threshold=train_threshold
        )   # 训练的时候要讲threshold设置得高一点，这样模型才有意愿好好学习，不然上来就松，后面就没法严了
        
        print(f"Epoch [{epoch+1}/{num_epochs}]")
        print(f"Train Loss: {train_loss:.4f} | Test Loss: {test_loss:.4f} | Recall @{train_threshold}: {test_recall:.4f}")
        
        # 根据召回率保存最佳模型
        if test_recall > best_recall:
            best_recall = test_recall
            torch.save(model.state_dict(), "best_model_recall.pth")
            print(f"Best model saved with Recall: {best_recall:.4f}")

    # 加载最佳模型
    model.load_state_dict(torch.load("best_model_recall.pth"))
    
    # 自动寻找最优阈值
    final_loss, final_recall, final_probas, final_labels, final_contents = test_model_with_content(
        model, test_loader, criterion, device, threshold=0.3
    )
    optimal_threshold = find_optimal_threshold(final_probas, final_labels, target_recall=0.95)
    print(f"\nOptimal Threshold: {optimal_threshold:.4f}")

    # 用最优阈值进行最终评估
    final_predictions = (final_probas > optimal_threshold).astype(float)
    final_recall = np.sum((final_predictions == 1) & (final_labels == 1)) / np.sum(final_labels == 1)
    print(f"Final Recall @{optimal_threshold:.2f}: {final_recall:.4f}")

    # 可视化结果
    plt.figure(figsize=(10, 6))
    positive_probas = final_probas[final_labels == 1]
    negative_probas = final_probas[final_labels == 0]
    plt.hist(positive_probas, bins=30, alpha=0.5, label="Positive", color="blue")
    plt.hist(negative_probas, bins=30, alpha=0.5, label="Negative", color="red")
    plt.axvline(optimal_threshold, color="green", linestyle="--", label=f"Threshold ({optimal_threshold:.2f})")
    plt.title("Probability Distribution")
    plt.xlabel("Probability")
    plt.ylabel("Count")
    plt.legend()
    plt.show()