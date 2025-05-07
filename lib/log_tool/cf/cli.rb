require 'thor'
require 'base64'
require 'securerandom'
require_relative '../common/aws_client'
require_relative '../common/logger'
require_relative '../common/utils'
require_relative '../common/config'
require_relative 'fetcher'
require_relative 'parser'

module LogTool
  module Cf
    class CLI < Thor
      default_task :fetch
      desc 'fetch', 'CloudFrontログを取得してCSVに変換'
      option :file,   type: :string,  desc: 'S3 または ローカルログファイル'
      option :start,  type: :string,  desc: '開始日時 (YYYY-MM-DD または YYYY-MM-DDThh:mm:ss [UTC])'
      option :end,    type: :string,  desc: '終了日時 (YYYY-MM-DD または YYYY-MM-DDThh:mm:ss [UTC])'
      option :output, type: :string,  default: 'cf_logs.csv', desc: '出力CSVファイル名'
      option :region, type: :string,  desc: 'AWS リージョン (省略時は .env から)'

      def fetch
        logger = Common::AppLogger.build

        begin
          # 設定の読み込み
          config = load_config

          # AWSクライアント初期化
          region = options[:region] || config['default']['region']
          client = Common::AwsClient.new(
            region: region
          ).s3_client

          # ログファイルの取得
          fetcher = Fetcher.new(client, config, options, logger)
          files = fetcher.list_files

          if files.empty?
            logger.error("対象ログファイルが見つかりませんでした")
            return
          end

          logger.info("#{files.size}件のログファイルが見つかりました")

          # ログファイルのダウンロードと解析
          raw_logs = fetcher.download(files)

          # CSV出力
          ensure_output_dir(config['default']['output_dir'])

          # 出力ファイル名の決定
          output_filename =
            if options[:output] == 'cf_logs.csv' # デフォルト値のまま
              generate_default_filename
            else
              options[:output]
            end

          output_path = File.join(config['default']['output_dir'], output_filename)

          parser = Parser.new(raw_logs, logger)
          parsed_count = parser.to_csv(output_path)

          logger.info("#{parsed_count}行のログをCSVに出力しました: #{output_path}")
        rescue => e
          logger.error("エラーが発生しました: #{e.message}")
          logger.debug(e.backtrace.join("\n"))
          exit 1
        end
      end

      private

      def load_config
        Common::Config.load
      end

      def ensure_output_dir(dir)
        Common::Utils.ensure_output_dir(dir)
      end

      # デフォルトのファイル名を生成（Base64エンコードを使用）
      def generate_default_filename
        # ユニークな識別子として現在時刻のUTCタイムスタンプなどを使用
        timestamp = Time.now.utc.strftime('%Y%m%d%H%M%S')
        # ランダム要素を追加してより一意性を高める
        random_suffix = SecureRandom.hex(4)
        identifier = "#{timestamp}_#{random_suffix}"
        # 識別子をBase64エンコード
        encoded = Base64.urlsafe_encode64(identifier, padding: false)
        "cf_#{encoded}.csv"
      end
    end
  end
end
