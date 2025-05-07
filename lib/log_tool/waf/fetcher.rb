require 'aws-sdk-s3'
require 'tempfile'
require 'date'
require 'pathname'
require_relative '../common/utils'

module LogTool
  module Waf
    class Fetcher
      def initialize(s3_client, config, options, logger)
        @s3_client = s3_client
        @config = config
        @options = options
        @logger = logger
        @bucket = config['waf']['s3_bucket']
        @prefix = config['waf']['s3_prefix']
      end

      # ログファイルの一覧を取得
      def list_files
        # ローカルファイルが指定されている場合
        if @options[:file]
          if File.exist?(@options[:file])
            return [@options[:file]]
          else
            @logger.error("指定されたファイル #{@options[:file]} が見つかりません")
            return []
          end
        end

        # 日付/日時範囲の設定
        start_datetime = parse_datetime(@options[:start])
        end_datetime = parse_datetime(@options[:end])

        # 時刻情報が含まれているかチェック
        has_time = Common::Utils.has_time_component?(@options[:start]) || Common::Utils.has_time_component?(@options[:end])

        @logger.info("期間 #{start_datetime} 〜 #{end_datetime} のログを検索")

        # S3バケットからのログファイル一覧取得
        files = []
        (start_datetime.to_date..end_datetime.to_date).each do |date|
          prefix = date_prefix(date)
          @logger.debug("S3バケット #{@bucket} のプレフィックス #{prefix} を検索")

          begin
            # S3オブジェクトリスト取得
            objects = @s3_client.list_objects_v2(bucket: @bucket, prefix: prefix).contents || []

            # 時刻情報がある場合はフィルタリング
            if has_time
              objects = filter_objects_by_time(objects, start_datetime, end_datetime)
            end

            objects.each do |object|
              files << { key: object.key, size: object.size }
            end
          rescue Aws::S3::Errors::NoSuchBucket
            @logger.error("S3バケット #{@bucket} が見つかりません")
            break
          end
        end

        @logger.info("#{files.size}件のファイルが見つかりました")
        files
      end

      # ログファイルをダウンロードして内容を返す
      def download(files)
        contents = []

        files.each do |file|
          if file.is_a?(Hash) && file[:key]
            # S3オブジェクトの場合
            @logger.info("S3からファイル #{file[:key]} をダウンロード中...")

            begin
              resp = @s3_client.get_object(bucket: @bucket, key: file[:key])
              content = resp.body.read
              contents << content
              @logger.info("ダウンロード成功: #{file[:key]} (#{content.bytesize} bytes)")
            rescue => e
              @logger.error("ダウンロード失敗: #{file[:key]} - #{e.message}")
            end
          else
            # ローカルファイルの場合
            local_path = file.is_a?(String) ? file : file[:key]
            @logger.info("ローカルファイル #{local_path} を読み込み中...")

            begin
              content = File.read(local_path)
              contents << content
              @logger.info("読み込み成功: #{local_path} (#{content.bytesize} bytes)")
            rescue => e
              @logger.error("読み込み失敗: #{local_path} - #{e.message}")
            end
          end
        end

        contents.join("\n")
      end

      private

      # 日時文字列をパース
      def parse_datetime(datetime_str)
        return DateTime.now.new_offset(0) unless datetime_str

        # Common::Utilsのparse_datetimeメソッドを使用
        Common::Utils.parse_datetime(datetime_str)
      end

      # S3オブジェクトを時刻でフィルタリング
      def filter_objects_by_time(objects, start_datetime, end_datetime)
        filtered_objects = []

        objects.each do |obj|
          # WAFログの場合、パスとファイル名からタイムスタンプを抽出
          # 例: AWSLogs/148189048278/WAFLogs/cloudfront/driver-wafacl-prd/2025/05/07/03/50/148189048278_waflogs_cloudfront_driver-wafacl-prd_20250507T0350Z_0a7915da.log.gz

          # パスから年/月/日/時/分を抽出
          path_match = obj.key.match(/(\d{4})\/(\d{2})\/(\d{2})\/(\d{2})\/(\d{2})/)

          # ファイル名からタイムスタンプを抽出 (YYYYMMDDTHHMMZ形式)
          timestamp_match = obj.key.match(/(\d{8})T(\d{2})(\d{2})Z/)

          if path_match
            year, month, day, hour, minute = path_match.captures.map(&:to_i)
            second = 0
            # UTCとしてDateTimeを作成
            obj_time = DateTime.new(year, month, day, hour, minute, second, 0)

            @logger.debug("ファイル #{obj.key} の時刻: #{obj_time}")

            # 時刻範囲内かチェック
            if obj_time >= start_datetime && obj_time <= end_datetime
              filtered_objects << obj
            end
          elsif timestamp_match
            # パスからの抽出が失敗した場合はファイル名のタイムスタンプを使用
            year = timestamp_match[1][0..3].to_i
            month = timestamp_match[1][4..5].to_i
            day = timestamp_match[1][6..7].to_i
            hour = timestamp_match[2].to_i
            minute = timestamp_match[3].to_i
            second = 0

            obj_time = DateTime.new(year, month, day, hour, minute, second, 0)

            @logger.debug("ファイル名から抽出した時刻: #{obj_time}")

            if obj_time >= start_datetime && obj_time <= end_datetime
              filtered_objects << obj
            end
          else
            # 時刻が抽出できない場合はlast_modifiedを使用
            if obj.last_modified >= start_datetime && obj.last_modified <= end_datetime
              filtered_objects << obj
            end
          end
        end

        filtered_objects
      end

      # 日付からS3プレフィックスを生成
      def date_prefix(date)
        formatted_date = date.strftime('%Y/%m/%d')
        "#{@prefix}#{formatted_date}/"
      end
    end
  end
end
