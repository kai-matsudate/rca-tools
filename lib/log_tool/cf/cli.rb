require 'thor'
require 'yaml'
require_relative '../common/aws_client'
require_relative '../common/logger'
require_relative '../common/utils'
require_relative 'fetcher'
require_relative 'parser'

module LogTool
  module Cf
    class CLI < Thor
      desc 'fetch', 'CloudFrontログを取得してCSVに変換'
      option :file,   type: :string,  desc: 'S3 または ローカルログファイル'
      option :start,  type: :string,  desc: '開始日 (YYYY-MM-DD)'
      option :end,    type: :string,  desc: '終了日 (YYYY-MM-DD)'
      option :output, type: :string,  default: 'cf_logs.csv', desc: '出力CSVファイル名'
      option :region, type: :string,  desc: 'AWS リージョン (省略時は config.yml から)'
      option :profile, type: :string, desc: 'AWS プロファイル名 (省略時はデフォルト)'

      def fetch
        logger = Common::AppLogger.build

        begin
          # 設定の読み込み
          config = load_config

          # AWSクライアント初期化
          region = options[:region] || config['default']['region']
          client = Common::AwsClient.new(
            profile: options[:profile],
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
          output_path = File.join(config['default']['output_dir'], options[:output])

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
        config_path = File.join(File.dirname(__FILE__), '../../../config/config.yml')
        unless File.exist?(config_path)
          raise "設定ファイルが見つかりません: #{config_path}"
        end
        YAML.load_file(config_path)
      end

      def ensure_output_dir(dir)
        Common::Utils.ensure_output_dir(dir)
      end
    end
  end
end
