require 'csv'
require 'fileutils'

module LogTool
  module Alb
    class Parser
      # ALBログのヘッダー定義
      HEADERS = [
        'type',
        'timestamp',
        'elb',
        'client_ip_port',
        'target_ip_port',
        'request_processing_time',
        'target_processing_time',
        'response_processing_time',
        'elb_status_code',
        'target_status_code',
        'received_bytes',
        'sent_bytes',
        'request',
        'user_agent',
        'ssl_protocol',
        'ssl_cipher',
        'target_group_arn',
        'trace_id',
        'domain_name',
        'chosen_cert_arn',
        'matched_rule_priority',
        'request_creation_time',
        'actions_executed',
        'redirect_url',
        'error_reason',
        'target_port_list',
        'target_status_code_list',
        'classification',
        'classification_reason'
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
              # ALBログはスペース区切り
              fields = parse_line(line)
              csv << fields
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

      # ALBログ行を解析
      def parse_line(line)
        # ALBログはスペース区切りだが、引用符内のスペースは保持する
        # 例: 2015-05-13T23:39:43.945958Z my-loadbalancer "GET /path/to/something?param=value HTTP/1.1" ...

        fields = []
        current_field = ''
        in_quotes = false

        line.chars.each do |char|
          if char == '"'
            in_quotes = !in_quotes
            current_field << char
          elsif char == ' ' && !in_quotes
            fields << current_field unless current_field.empty?
            current_field = ''
          else
            current_field << char
          end
        end

        fields << current_field unless current_field.empty?

        # 引用符の処理（前後の引用符を削除）
        fields.map do |field|
          if field.start_with?('"') && field.end_with?('"')
            field[1..-2]
          else
            field
          end
        end
      end

      def ensure_output_dir(dir)
        FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
      end
    end
  end
end
