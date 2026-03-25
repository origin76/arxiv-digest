# arXiv Digest

这个仓库现在包含两条独立链路：

- `main.py`: arXiv 论文日报
- `macro_main.py`: 宏观信号压缩日报

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
- 对待评估论文在 LLM 评估前批量并发调用 OpenAlex，按作者名补作者单位
- 对 LLM 相关性评估和 summary 阶段做受控并发，减少串行等待
- 支持 `DRY_RUN`，可以先看结果不发邮件
- 自动生成详细日志和调试产物
- GitHub Actions 支持定时运行
- 新增独立的 Macro Signal Extractor，用来压缩宏观/地缘、大宗、国债、板块、外汇信号

## 项目结构

```text
.
├── .github/workflows/daily.yml   # GitHub Actions 定时任务
├── main.py                       # 薄入口，负责启动 pipeline
├── macro_main.py                 # 宏观日报入口
├── digest_pipeline.py            # 主流程编排
├── digest_config.py              # 环境变量与运行时配置
├── digest_runtime.py             # 日志、artifact、LLM client
├── digest_sources.py             # arXiv/OpenAlex/作者元数据
├── digest_llm.py                 # 评估、摘要、LLM 并发
├── digest_email.py               # 邮件渲染与 SMTP 发送
├── macro_pipeline.py             # 宏观日报主流程编排
├── macro_config.py               # 宏观日报配置
├── macro_sources.py              # 宏观新闻与市场数据抓取
├── macro_llm.py                  # 宏观日报 LLM 压缩
├── macro_email.py                # 宏观日报邮件渲染
├── macro_prompts.py              # 宏观日报 prompt
├── prompts.py                    # LLM prompt
├── requirements.txt              # Python 依赖
├── seen_ids.json                 # 已处理论文 ID
├── local.env.sh                  # 本地环境变量脚本（已被 gitignore）
├── openalex_cache.json           # OpenAlex 作者单位缓存（已被 gitignore）
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
| `LLM_ASSESS_MAX_WORKERS` | 相关性评估阶段的并发线程数，默认 8 |
| `LLM_SUMMARY_MAX_WORKERS` | summary 阶段的并发线程数，默认 4 |
| `OPENALEX_ENRICHMENT_ENABLED` | 是否在评估前按作者名用 OpenAlex 补作者单位，默认 `true` |
| `OPENALEX_TIMEOUT_SECONDS` | OpenAlex 请求超时时间，默认 15 秒 |
| `OPENALEX_MAX_WORKERS` | OpenAlex 并发线程数，默认 8 |
| `OPENALEX_EMAIL` | 可选。只有你想显式标识调用方时才设置；默认不传 `mailto` |

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

## 宏观日报

新增的 `Macro Signal Extractor` 是一条独立链路，目标不是罗列新闻，而是只保留“会改变世界状态”的宏观信号。

固定覆盖 5 个模块：

- Macro / Geopolitics
- Commodities
- Rates / Sovereign Bonds
- Equities by sector
- FX

当前默认数据设计：

- 新闻面：Google News RSS 搜索聚合，按模块抓取高相关 headline
- 市场面：默认优先用 `yfinance` 拉这 12 个市场标的；若部分缺失，再回退到 Stooq 和 Frankfurter；DXY 优先用 `DX-Y.NYB`，失败时再按 6 币种权重公式推导；Yahoo 原生接口默认关闭，仅在显式开启时做补缺；FRED 为主，Treasury 官方 CSV 与当前月页面为备份抓美债 2Y / 10Y
- 压缩层：LLM 将新闻和跨资产快照合并成一封短而高信号的日报，并输出中英双语摘要

### 宏观日报环境变量

除了通用变量外，宏观链路还支持这些配置：

| 变量 | 说明 |
| --- | --- |
| `MACRO_EMAIL_TO` | 可选。宏观日报单独收件人；不设时回退到 `EMAIL_TO` |
| `MACRO_NEWS_LOOKBACK_HOURS` | 新闻回看窗口，默认 36 小时 |
| `MACRO_MAX_HEADLINES_PER_BUCKET` | 每个模块最多保留多少条 headline，默认 8 |
| `MACRO_NEWS_MAX_WORKERS` | 新闻抓取并发数，默认 8 |
| `MACRO_NEWS_TIMEOUT_SECONDS` | 单个新闻源超时，默认 15 秒 |
| `MACRO_NEWS_RETRIES` | Google News RSS 重试次数，默认 2 |
| `MACRO_MARKET_TIMEOUT_SECONDS` | 市场数据超时，默认 15 秒 |
| `MACRO_MARKET_RETRIES` | 市场数据重试次数，默认 3 |
| `MACRO_RATES_MAX_AGE_DAYS` | 利率快照允许的最大陈旧天数，默认 10，过旧数据会被拒绝 |
| `FRED_MAX_RETRIES` | FRED 重试次数，默认 1，失败后更快切到 Treasury 备用源 |
| `STOOQ_MAX_RETRIES` | Stooq 请求重试次数，默认 2 |
| `STOOQ_MAX_WORKERS` | Stooq 并发数，默认 2 |
| `YAHOO_ENABLED` | 是否启用 Yahoo 作为补缺源，默认 `false` |
| `YAHOO_MAX_RETRIES` | Yahoo 请求重试次数，默认 3 |
| `YAHOO_CHART_MAX_WORKERS` | Yahoo chart fallback 并发数，默认 1 |

### 本地 dry run

```bash
source .venv/bin/activate
source ./local.env.sh
export DRY_RUN=true
python macro_main.py
```

### 真正发信

```bash
source .venv/bin/activate
source ./local.env.sh
export DRY_RUN=false
python macro_main.py
```

### 宏观日报产物

每次运行会额外生成这些调试文件：

- `macro_config.json`
- `macro_news_inputs.json`
- `macro_market_snapshot.json`
- `macro_report.json`
- `macro_pipeline_summary.json`
- `macro_email_preview.html`

如果某个新闻源或市场源失败，链路会尽量降级继续跑，并把失败信息记到 artifact 和日志里。

## 日志与调试

每次运行都会在 `logs/<timestamp>/` 下生成产物。

重点文件：

- `run.log`
- `pipeline_summary.json`
- `paper_assessments.json`
- `openalex_enrichment.json`
- `ranked_candidates.json`
- `selected_papers.json`
- `email_preview.html`

如果打开了 `LOG_RAW_LLM=true`，还会额外生成：

- `llm/*-prompt.txt`
- `llm/*-response.txt`

推荐的调试顺序：

1. 先用 `DRY_RUN=true`
2. 先看 `pipeline_summary.json`
3. 再看 `openalex_enrichment.json`
4. 然后看 `ranked_candidates.json`
5. 最后看 `email_preview.html`

`openalex_enrichment.json` 里会记录每篇待评估论文的：

- 每个缺失 affiliation 的作者是否命中缓存
- 每个作者是否命中唯一可信的 OpenAlex author
- 命中的 author id / name / works_count
- 每个作者最终补到了什么单位
- 实际补了几个作者单位

OpenAlex 现在会在 LLM 判断相关性之前统一批量跑完，并且会对作者名去重后再并发请求，避免串行网络等待成为主瓶颈。

LLM 的相关性评估和 summary 阶段现在都支持受控并发，默认分别使用 `LLM_ASSESS_MAX_WORKERS=8` 和 `LLM_SUMMARY_MAX_WORKERS=4`。如果你的模型限流比较紧，可以把它们调低。

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
