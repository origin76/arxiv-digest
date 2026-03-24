# arXiv Digest

一个自动化论文 digest 工具：每天抓取 arXiv 上昨天发布的论文，在给定范围内做筛选和打分，选出最值得读的 Top 10，生成摘要邮件并发送。

当前 digest 的关注范围是：

- General OS
- General AI infra / MLSys
- AI 编译
- 编译器设计与实现
- 程序分析

同时会尽量排除这些相关度不高的内容：

- FPGA / ASIC 为主的硬件架构论文
- 边缘计算、嵌入式部署、DVFS、功耗优化
- 联邦学习、隐私、安全计算
- AI4Science / 垂直领域应用
- 纯模型结构、纯 benchmark、纯算法改进

## 功能概览

- 抓取昨天的 arXiv 论文，而不是只看固定几篇
- 使用 DashScope 兼容 OpenAI API 的模型做评估和摘要
- 先判断论文是否在 digest 范围内，再按“是否值得认真读”打分
- 对通过筛选的论文排序，最终只发送 Top 10
- 支持 `DRY_RUN`，可以先看结果不发邮件
- 自动生成详细日志和调试产物
- GitHub Actions 支持定时运行

## 项目结构

```text
.
├── .github/workflows/daily.yml   # GitHub Actions 定时任务
├── main.py                       # 主流程：抓取、评估、排序、摘要、发邮件
├── prompts.py                    # LLM prompt
├── requirements.txt              # Python 依赖
├── seen_ids.json                 # 已处理论文 ID
├── local.env.sh                  # 本地环境变量脚本（已被 gitignore）
└── logs/                         # 调试日志与产物（已被 gitignore）
```

## 本地运行

### 1. 创建虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置环境变量

项目默认使用 `local.env.sh` 来管理本地环境变量：

```bash
source ./local.env.sh
```

你至少需要确认这些变量是正确的：

- `DASHSCOPE_API_KEY`
- `EMAIL_USER`
- `EMAIL_PASS`
- `EMAIL_TO`
- `DRY_RUN`

常用变量说明：

| 变量 | 说明 |
| --- | --- |
| `DASHSCOPE_API_KEY` | DashScope API Key |
| `EMAIL_USER` | 发件邮箱 |
| `EMAIL_PASS` | 发件邮箱密码 |
| `EMAIL_TO` | 收件邮箱 |
| `EMAIL_SMTP_HOST` | SMTP 主机 |
| `EMAIL_SMTP_PORT` | SMTP 端口 |
| `EMAIL_USE_SSL` | 是否使用 SMTP SSL |
| `EMAIL_USE_STARTTLS` | 是否使用 STARTTLS |
| `DRY_RUN` | `true` 时只生成结果，不发邮件、不写回状态 |
| `LOG_LEVEL` | 日志级别，建议本地调试用 `DEBUG` |
| `LOG_RAW_LLM` | 是否保存原始 prompt/response |
| `MAX_SELECTED_PAPERS` | 最终发送的 Top N，默认 10 |
| `ARXIV_PAGE_SIZE` | 每页抓取多少篇，默认 100 |
| `TARGET_DAYS_AGO` | 抓取几天前的论文，默认 1 表示昨天 |
| `LOCAL_TIMEZONE` | 本地时区，默认 `Asia/Shanghai` |
| `LLM_MODEL` | 评估和摘要使用的模型 |
| `LLM_TIMEOUT_SECONDS` | 单次 LLM 请求超时时间 |

### 3. 本地 dry run

建议先 dry run：

```bash
source .venv/bin/activate
source ./local.env.sh
export DRY_RUN=true
python main.py
```

### 4. 真正发信

确认结果没问题后再发邮件：

```bash
source .venv/bin/activate
source ./local.env.sh
export DRY_RUN=false
python main.py
```

## 日志与调试

每次运行都会在 `logs/<timestamp>/` 下生成产物。

重点文件：

- `run.log`
- `pipeline_summary.json`
- `paper_assessments.json`
- `ranked_candidates.json`
- `selected_papers.json`
- `email_preview.html`

如果打开了 `LOG_RAW_LLM=true`，还会额外生成：

- `llm/*-prompt.txt`
- `llm/*-response.txt`

推荐的调试顺序：

1. 先用 `DRY_RUN=true`
2. 先看 `pipeline_summary.json`
3. 再看 `ranked_candidates.json`
4. 最后看 `email_preview.html`

## 部署到 GitHub

如果当前目录还不是 Git 仓库，可以这样初始化并推送：

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <你的 GitHub 仓库地址>
git push -u origin main
```

如果你已经创建了 GitHub 仓库，常见 remote 形式是：

```bash
git remote add origin git@github.com:<your-name>/<repo-name>.git
```

或者：

```bash
git remote add origin https://github.com/<your-name>/<repo-name>.git
```

之后正常更新代码：

```bash
git add .
git commit -m "Update digest pipeline"
git push
```

## GitHub Actions 配置

仓库已经包含定时任务：

- 文件：[.github/workflows/daily.yml](/Users/zerick/code/arxiv-digest/.github/workflows/daily.yml)
- 触发方式：
  - 每天 UTC `00:00`
  - 手动 `workflow_dispatch`

这个时间对应北京时间大约早上 8 点。

### 需要配置的 GitHub Secrets

在 GitHub 仓库里进入：

`Settings -> Secrets and variables -> Actions`

添加这些 secrets：

- `DASHSCOPE_API_KEY`
- `EMAIL_USER`
- `EMAIL_PASS`
- `EMAIL_TO`

当前 workflow 里邮件服务器已经写成：

- `mail.tiaozhan.com`
- `465`
- `SSL`

如果以后邮箱服务变了，可以直接改 [daily.yml](/Users/zerick/code/arxiv-digest/.github/workflows/daily.yml)。

## 状态文件

项目会把处理过的论文 ID 写到：

- `seen_ids.json`

这样下一次运行时不会重复处理同一批论文。  
GitHub Actions 也会尝试自动提交这个文件，所以 workflow 里已经加了 `contents: write` 权限。

## 推荐的首次上线流程

1. 本地 `DRY_RUN=true` 跑通
2. 本地 `DRY_RUN=false` 实际发一封测试邮件
3. 推到 GitHub
4. 配好 Actions secrets
5. 在 GitHub 上手动运行一次 `workflow_dispatch`
6. 确认邮件正常、`seen_ids.json` 能自动回写

## 依赖

```bash
pip install -r requirements.txt
```

当前依赖很少：

- `feedparser`
- `openai`
