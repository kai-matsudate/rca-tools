require 'csv'
require 'fileutils'
require 'json'

module LogTool
  module Waf
    class Parser
      # WAFログのヘッダー定義
      # 主要なフィールドをヘッダーに定義
      HEADERS = [
        'timestamp',
        'formatVersion',
        'webaclId',
        'terminatingRuleId',
        'terminatingRuleType',
        'action',
        'httpSourceName',
        'httpSourceId',
        'clientIp',
        'country',
        'uri',
        'args',
        'httpMethod',
        'requestId',
        'httpVersion',
        'headers',
        'labels',
        'ja3Fingerprint',
        'ja4Fingerprint'
      ]

      def initialize(content, logger)
        @raw_content = content
        @logger = logger
      end

      # ログをCSVに変換
      def to_csv(output_path)
        ensure_output_dir(File.dirname(output_path))

        parsed_count = 0
        error_count = 0

        CSV.open(output_path, 'w', headers: HEADERS, write_headers: true) do |csv|
          @raw_content.each_line do |line|
            line = line.strip
            next if line.empty?

            begin
              # WAFログはJSON形式
              record = parse_json_line(line)
              csv << record
              parsed_count += 1
            rescue => e
              @logger.warn("ログ行の解析に失敗しました: #{e.message}")
              @logger.debug("問題のある行: #{line}")
              error_count += 1
            end
          end
        end

        @logger.info("処理完了: 成功=#{parsed_count}, 失敗=#{error_count}")
        parsed_count
      end

      private

      def parse_json_line(line)
        data = JSON.parse(line)
        record = []

        # 主要フィールドの取得
        record << data['timestamp'].to_s
        record << data['formatVersion'].to_s
        record << data['webaclId'].to_s
        record << data['terminatingRuleId'].to_s
        record << data['terminatingRuleType'].to_s
        record << data['action'].to_s
        record << data['httpSourceName'].to_s
        record << data['httpSourceId'].to_s

        # HTTPリクエスト情報の取得
        http_req = data['httpRequest'] || {}
        record << http_req['clientIp'].to_s
        record << http_req['country'].to_s
        record << http_req['uri'].to_s
        record << http_req['args'].to_s
        record << http_req['httpMethod'].to_s
        record << http_req['requestId'].to_s
        record << http_req['httpVersion'].to_s

        # ヘッダー情報をJSON文字列として格納
        headers = http_req['headers'] || []
        record << headers.to_json

        # ラベル情報をJSON文字列として格納
        labels = data['labels'] || []
        record << labels.to_json

        # Ja3/Ja4フィンガープリント
        record << data['ja3Fingerprint'].to_s
        record << data['ja4Fingerprint'].to_s

        record
      end

      def ensure_output_dir(dir)
        FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
      end
    end
  end
end
