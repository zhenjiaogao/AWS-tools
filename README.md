# RDS/Aurora 成本分析工具集

面向 AWS SA 和客户的一键式成本分析工具，帮助评估 RDS/Aurora 的迁移、换代和 Extended Support 成本。

---

## 工具列表

| 脚本 | 用途 |
|------|------|
| `rds_aurora_multi_generation_pricing_analyzer.py` | RDS → Aurora 迁移 / RDS 机型替换 / Aurora Graviton 换代评估 |
| `rds_extended_support_pricing_analyzer.py` | RDS/Aurora Extended Support 费用评估（EOS 时间线 + 逐年费用） |

---

## 1. RDS/Aurora 多代实例定价分析器

### 功能概述

三种分析模式，可独立执行或全量运行：

| 模式 | 说明 |
|------|------|
| `--mode rds-to-aurora` | 评估 RDS MySQL/PG 迁移到 Aurora（IO-Optimized）的成本 |
| `--mode rds-replacement` | 评估 RDS 留在 RDS、升级 Graviton 机型的成本 |
| `--mode aurora-replacement` | 评估现有 Aurora Provisioned 实例升级 Graviton 的成本（仅 instance cost） |

### 前置条件

**Python 环境**：
```bash
pip install boto3 pandas openpyxl
```

**IAM 权限**（最小权限策略）：
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "rds:DescribeDBInstances",
      "rds:DescribeDBClusters",
      "cloudwatch:GetMetricStatistics",
      "pricing:GetProducts",
      "sts:GetCallerIdentity"
    ],
    "Resource": "*"
  }]
}
```

> **注意**：`pricing:GetProducts` 固定访问 `us-east-1` endpoint，请确保 IAM 策略不限制该 region。

### 使用方法

```bash
# 全量分析（三个模式同时执行）
python rds_aurora_multi_generation_pricing_analyzer.py <region>

# 仅 RDS → Aurora 迁移评估（MySQL）
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 --mode rds-to-aurora --engine mysql

# 仅 RDS 机型替换评估（PostgreSQL）
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 --mode rds-replacement --engine postgresql

# 仅 Aurora Provisioned Graviton 换代评估
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 --mode aurora-replacement --engine mysql

# 大量实例时启用并发模式
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 --optimize -w 20 -b 100

# 指定输出文件名
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 -o my_report.xlsx
```

**EC2 执行**（使用 Instance Profile）：
```bash
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1
```

**本地执行**（使用 SSO Profile）：
```bash
aws sso login --profile your-profile
export AWS_PROFILE=your-profile
python rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1
```

### 参数说明

| 参数 | 必填 | 说明 |
|------|------|------|
| `region` | ✅ | 目标 AWS 区域（如 ap-southeast-1, us-east-1） |
| `--mode` | ❌ | 分析模式：`rds-to-aurora` / `rds-replacement` / `aurora-replacement` / `all`（默认 all） |
| `--engine` | ❌ | 引擎类型：`mysql` / `postgresql` / `all`（默认 all） |
| `--optimize` | ❌ | 启用并发模式（推荐实例 >20 个时开启） |
| `-w / --workers` | ❌ | 并发线程数（默认 10，上限 50） |
| `-b / --batch-size` | ❌ | 每批实例数（默认 50） |
| `-o / --output` | ❌ | 输出文件名（默认自动生成） |

### 输出文件

生成一个 Excel 文件（`.xlsx`），包含以下 Sheet：

**`--mode rds-to-aurora` 或 `--mode rds-replacement`**：

| Sheet | 内容 |
|-------|------|
| MySQL集群汇总 | RDS MySQL 集群级成本对比 |
| MySQL详细数据 | 每个实例的完整明细 |
| PG集群汇总 | RDS PostgreSQL 集群级成本对比 |
| PG详细数据 | PostgreSQL 实例明细 |

**`--mode aurora-replacement`**：

| Sheet | 内容 |
|-------|------|
| Aurora MySQL Graviton汇总 | 集群级 Graviton 换代成本汇总 |
| Aurora MySQL Graviton详细 | 每实例 r6g/r7g/r8g 成本对比 |
| Aurora PG Graviton汇总 | PostgreSQL 集群级汇总 |
| Aurora PG Graviton详细 | PostgreSQL 实例明细 |

### 定价逻辑

| 项目 | 计算方式 |
|------|---------|
| RDS 实例费 | 1yr RI No Upfront hourly × 730（MAZ ×2） |
| RDS 存储费 | allocated_gb × 单价 + IOPS 费（MAZ ×2） |
| Aurora IO-Opt 实例费 | Standard RI hourly × 1.3 × 730 × instance_count |
| Aurora IO-Opt 存储费 | used_storage_gb × Standard 存储单价 × 2.25 |
| Aurora Graviton 换代 | IO-Opt 实例费对比（不含存储） |

### 支持的 RDS 架构

| 架构 | 检测方式 | RDS 实例费倍数 | Aurora 节点映射 |
|------|---------|---------------|----------------|
| Single-AZ | MultiAZ=false, 无 Replica | ×1 | 1 Writer |
| Multi-AZ | MultiAZ=true | ×2 | 2 节点（无 Replica 时） |
| TAZ Cluster | 3 成员跨 3 AZ | ×1（每节点独立） | 各 1 节点 |
| Read Replica | 有 source_db_instance_identifier | ×1 | 1 Reader |

### 限制

- 仅支持 Global Region 执行（不支持 China Region）
- 定价基于 1yr RI No Upfront，非 On-Demand
- Aurora Graviton 换代仅计算 instance cost，不含存储
- T 系列映射到 Aurora 时统一升为 `large`（Aurora 最小规格）

---

## 2. RDS/Aurora Extended Support 定价分析器

### 功能概述

分析账户中 RDS/Aurora 实例的 Extended Support (ES) 费用：
- 识别各引擎版本的 EOS（End of Standard Support）时间
- 标注紧急度（已进入 ES / 即将进入 / 安全）
- 按 pricing year（Year 1/2/3）计算逐年 ES 费用
- 集群维度汇总

### 前置条件

**Python 环境**：
```bash
pip install boto3
```

**IAM 权限**：
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "rds:DescribeDBInstances",
      "rds:DescribeDBClusters",
      "pricing:GetProducts",
      "sts:GetCallerIdentity"
    ],
    "Resource": "*"
  }]
}
```

### 使用方法

```bash
# 分析指定 region
python rds_extended_support_pricing_analyzer.py ap-southeast-1

# 指定引擎
python rds_extended_support_pricing_analyzer.py ap-southeast-1 --engine mysql

# 指定输出文件
python rds_extended_support_pricing_analyzer.py ap-southeast-1 -o es_report.xlsx
```

**EC2 执行**：
```bash
python rds_extended_support_pricing_analyzer.py ap-southeast-1
```

**本地 SSO 执行**：
```bash
export AWS_PROFILE=your-profile
python rds_extended_support_pricing_analyzer.py ap-southeast-1
```

### 输出文件

生成 Excel（`.xlsx`）和 HTML 报告，包含：
- 实例明细（版本、EOS 日期、ES 费用逐年明细）
- 集群汇总（按 cluster 聚合的年度 ES 费用）
- 紧急度标注（红/黄/绿）

---

## 部署到 EC2

### 快速部署

```bash
# 1. 安装 Python（Amazon Linux 2023 已预装）
sudo dnf install python3-pip -y

# 2. 安装依赖
pip3 install boto3 pandas openpyxl

# 3. 上传脚本（或 git clone）
# 确保 EC2 Instance Profile 有上述 IAM 权限

# 4. 执行
python3 rds_aurora_multi_generation_pricing_analyzer.py ap-southeast-1 --mode rds-to-aurora
python3 rds_extended_support_pricing_analyzer.py ap-southeast-1
```

### IAM Role 配置

创建 EC2 Instance Profile，附加以下策略：

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "rds:DescribeDBInstances",
      "rds:DescribeDBClusters",
      "cloudwatch:GetMetricStatistics",
      "pricing:GetProducts",
      "sts:GetCallerIdentity"
    ],
    "Resource": "*"
  }]
}
```

---

## 项目结构

```
.
├── README.md
├── rds_aurora_multi_generation_pricing_analyzer.py   # 迁移/换代成本分析
└── rds_extended_support_pricing_analyzer.py          # Extended Support 费用分析
```

---

## 架构与处理流程

### Pricing Analyzer 执行流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          用户执行脚本                                      │
│  python rds_aurora_multi_generation_pricing_analyzer.py <region> --mode  │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 1: 区域兼容性检查                                                   │
│  - EC2 Metadata → 判断当前环境是否为 China Region                         │
│  - China Region EC2 → 拒绝执行                                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 2: 批量获取定价（Pricing API, us-east-1 endpoint）                   │
│                                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐     │
│  │ RDS MySQL/PG    │  │ Aurora MySQL/PG  │  │ Storage (gp2/gp3/   │     │
│  │ Instance Pricing│  │ Instance Pricing │  │ io1/io2/magnetic)    │     │
│  │ (Single-AZ,    │  │ (Standard,       │  │                      │     │
│  │  1yr RI NoUp)  │  │  1yr RI NoUp)    │  │ + Aurora Storage/IO  │     │
│  └────────┬────────┘  └────────┬─────────┘  └──────────┬───────────┘     │
│           │                    │                        │                 │
│           ▼                    ▼                        ▼                 │
│  ┌──────────────────────────────────────────────────────────────┐        │
│  │              pricing_cache / aurora_pricing_cache             │        │
│  │              storage_pricing_cache (内存缓存)                  │        │
│  └──────────────────────────────────────────────────────────────┘        │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 3: 实例发现（目标 Region）                                           │
│                                                                           │
│  ┌─────────────────────────┐    ┌──────────────────────────────┐        │
│  │ rds:DescribeDBInstances │    │ rds:DescribeDBClusters       │        │
│  │ - 分页获取所有实例        │    │ - 获取 Aurora 集群信息         │        │
│  │ - 过滤: engine, status   │    │ - TAZ 架构检测               │        │
│  │ - 排除: serverless       │    │ - Writer/Reader 角色识别      │        │
│  └────────────┬────────────┘    └──────────────┬───────────────┘        │
│               │                                │                         │
│               ▼                                ▼                         │
│  ┌──────────────────────────────────────────────────────────────┐        │
│  │         架构判定 (SAZ / MAZ / TAZ / Read Replica)             │        │
│  │         集群关系识别 (Primary ↔ Replica)                       │        │
│  └──────────────────────────────────────────────────────────────┘        │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 4: CloudWatch 指标采集（每实例）                                      │
│                                                                           │
│  cloudwatch:GetMetricStatistics                                          │
│  - CPUUtilization: avg/max/min (1Hr 粒度, 15 天)                          │
│  - ReadIOPS + WriteIOPS: avg (1Hr 粒度, 15 天)                            │
│  - FreeStorageSpace: 计算 used_storage_gb                                 │
│                                                                           │
│  [--optimize 模式: ThreadPoolExecutor 并发采集]                            │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 5: 成本计算                                                         │
│                                                                           │
│  ┌─ rds-to-aurora ──────────────────────────────────────────────┐        │
│  │  Aurora IO-Opt Instance = Standard RI × 1.3 × 730 × count   │        │
│  │  Aurora IO-Opt Storage  = used_gb × standard_price × 2.25    │        │
│  │  Aurora IO-Opt Total    = Instance + Storage                 │        │
│  └──────────────────────────────────────────────────────────────┘        │
│                                                                           │
│  ┌─ rds-replacement ────────────────────────────────────────────┐        │
│  │  RDS Replacement = target RI hourly × 730 (MAZ: ×2)         │        │
│  │  + 原始 RDS 存储成本（不变）                                    │        │
│  └──────────────────────────────────────────────────────────────┘        │
│                                                                           │
│  ┌─ aurora-replacement ─────────────────────────────────────────┐        │
│  │  Current IO-Opt = Standard RI × 1.3 × 730                   │        │
│  │  Target IO-Opt  = Target Standard RI × 1.3 × 730            │        │
│  │  Saving = Current - Target (仅 instance cost)                │        │
│  └──────────────────────────────────────────────────────────────┘        │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Step 6: 输出 Excel                                                       │
│  - 集群汇总 Sheet（按 Primary 聚合）                                       │
│  - 实例详细 Sheet（每实例一行，全字段）                                      │
│  - Aurora Graviton 汇总/详细 Sheet                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### API 调用清单

| AWS API | 调用 Region | 用途 |
|---------|------------|------|
| `sts:GetCallerIdentity` | 目标 region | 获取 Account ID |
| `pricing:GetProducts` | **us-east-1**（固定） | 获取 RDS/Aurora 实例定价、存储定价 |
| `rds:DescribeDBInstances` | 目标 region | 发现 RDS/Aurora 实例 |
| `rds:DescribeDBClusters` | 目标 region | 获取集群信息、TAZ 检测、Writer/Reader 识别 |
| `cloudwatch:GetMetricStatistics` | 目标 region | 采集 CPU / IOPS / FreeStorageSpace |
| EC2 Instance Metadata (169.254.169.254) | 本地 | 检测运行环境区域（非 EC2 时跳过） |

### Extended Support 分析器执行流程

```
用户执行脚本
     │
     ▼
┌──────────────────────────────────────┐
│ 1. pricing:GetProducts (us-east-1)   │  获取 ES 费率（per vCPU/hr by year）
│ 2. rds:DescribeDBInstances           │  发现实例 + 版本信息
│ 3. rds:DescribeDBClusters            │  集群聚合
│ 4. 匹配 EOS 时间表                     │  内置版本 → EOS 日期映射
│ 5. 计算逐年 ES 费用                    │  vCPU × hourly_rate × 730 × year
│ 6. 输出 Excel + HTML                  │
└──────────────────────────────────────┘
```

---

## FAQ

**Q: 支持 China Region 吗？**  
A: Extended Support 脚本支持。Pricing Analyzer 仅支持 Global Region（脚本启动时会自动检测并拒绝 China Region）。

**Q: 定价数据从哪来？**  
A: 通过 AWS Pricing API 实时获取（endpoint 在 us-east-1），不依赖本地定价文件。

**Q: 可以跨账户分析吗？**  
A: 脚本分析当前凭证所属账户的实例。如需跨账户，请使用 assume-role 或切换 profile 分别执行。

**Q: 并发模式会触发 API 限流吗？**  
A: 可能。建议 `-w` 不超过 30，`-b` 保持默认 50。遇到限流时脚本会自动重试。

**Q: T 系列实例在 Aurora 迁移评估中怎么处理？**  
A: T 系列的 micro/small/medium 统一映射到 Aurora `large`（Aurora 最小规格）。T 系列不参与 Aurora Graviton 换代评估（Aurora 无 T 系列）。
