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
bin/cf_to_csv fetch --start 2025-05-01 --end 2025-05-07 --output cf.csv
```

## 設定

config/config.yml を編集してバケット名・プレフィックス・リージョンを指定してください。
