# RcaTools

## 概要

RcaToolsは、AWS環境でのRoot Cause Analysis（原因分析）を支援するためのログ収集・変換ツールです。ALB、CloudFront、WAFの各種ログをS3から効率的に取得し、分析しやすいCSV形式に変換します。

### 主な機能

- **複数のAWSサービスに対応**: ALB、CloudFront、WAFのログをサポート
- **柔軟なログ取得**: S3バケットからの一括取得、単一ファイル指定、ローカルファイル処理
- **日時範囲指定**: 特定の期間のログのみを抽出可能
- **CSV変換**: 複雑なログ形式を分析しやすいCSV形式に変換

障害対応やパフォーマンス分析、セキュリティインシデント調査など、様々なシナリオでの原因究明をサポートします。

## インストール

```bash
git clone https://github.com/kai-matsudate/rca_tools.git && cd rca_tools
bundle install
```

## 使い方

### ALB ログ取得

```bash
bin/alb_to_csv --file s3://bucket/path/log.gz --output alb.csv

# 日時範囲指定 - UTC時間
bin/alb_to_csv --start 2025-05-01T12:00:00 --end 2025-05-07T18:30:00 --output alb.csv
```

### CloudFront ログ取得

```bash
# 日時範囲指定 - UTC時間
bin/cf_to_csv --start 2025-05-01T12:00:00 --end 2025-05-07T18:30:00 --output cf.csv

# 単一ファイル指定（S3）
bin/cf_to_csv --file s3://my-cf-logs-bucket/path/to/log.gz --output cf.csv

# ローカルファイル指定
bin/cf_to_csv --file /path/to/local/cf-log.gz --output cf.csv
```

### WAF ログ取得

```bash
# 日時範囲指定 - UTC時間
bin/waf_to_csv --start 2025-05-01T12:00:00 --end 2025-05-07T18:30:00 --output waf.csv

# 単一ファイル指定（S3）
bin/waf_to_csv --file s3://my-waf-logs-bucket/path/to/log --output waf.csv

# ローカルファイル指定
bin/waf_to_csv --file /path/to/local/waf-log --output waf.csv
```

## 設定

プロジェクトルートに`.env`ファイルを作成し、バケット名・プレフィックス・リージョンを指定してください。

```bash
# .envファイル例
DEFAULT_REGION=us-east-1
OUTPUT_DIR=./output

ALB_BUCKET=my-alb-logs-bucket
ALB_PREFIX=AWSLogs/123456789012/elasticloadbalancing/us-east-1/app/my-alb/

CF_BUCKET=my-cf-logs-bucket
CF_PREFIX=mydistribution-logs/

WAF_S3_BUCKET=my-waf-logs-bucket
WAF_S3_PREFIX=AWSLogs/123456789012/waf/us-east-1/
```

`.env.example`ファイルをコピーして使用することもできます。
