# LogTool

## インストール

```bash
git clone https://github.com/kai-matsudate/log_tool.git && cd log_tool
bundle install
```

## 使い方

### ALB ログ取得

```bash
bin/alb_to_csv fetch --file s3://bucket/path/log.gz --output alb.csv
bin/alb_to_csv fetch --start 2025-05-01 --end 2025-05-07 --output alb.csv
```

### CloudFront ログ取得

```bash
# 日付範囲指定
bin/cf_to_csv fetch --start 2025-05-01 --end 2025-05-07 --output cf.csv

# 単一ファイル指定（S3）
bin/cf_to_csv fetch --file s3://my-cf-logs-bucket/path/to/log.gz --output cf.csv

# ローカルファイル指定
bin/cf_to_csv fetch --file /path/to/local/cf-log.gz --output cf.csv
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
```

`.env.example`ファイルをコピーして使用することもできます。
