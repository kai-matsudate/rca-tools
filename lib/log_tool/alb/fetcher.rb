require 'uri'
require_relative '../common/utils'

module LogTool
  module Alb
    class Fetcher
      def initialize(s3_client, config, options, logger)
        @s3_client = s3_client
        @config = config
        @options = options
        @logger = logger
        @alb_config = config['alb']
      end

      # ファイルリストの取得
      def list_files
        if @options[:file]
          # 単一ファイル指定モード
          file_uri = URI.parse(@options[:file])
          if file_uri.scheme == 's3'
            @logger.info("S3から単一ファイルを取得します: #{@options[:file]}")
            # s3://bucket/path/to/file.gz の形式から bucket と key を抽出
            bucket = file_uri.host
            key = file_uri.path.sub(/^\//, '')
            [{
              bucket: bucket,
              key: key,
              local: false
            }]
          else
            @logger.info("ローカルファイルを処理します: #{@options[:file]}")
            # ローカルファイル
            [{
              path: @options[:file],
              local: true
            }]
          end
        elsif @options[:start] && @options[:end]
          # 日付/日時範囲指定モード
          @logger.info("期間指定でログを取得します: #{@options[:start]} から #{@options[:end]}")

          # 日付プレフィックスと日時情報を取得
          date_prefixes, start_datetime, end_datetime = Common::Utils.date_prefixes(@options[:start], @options[:end])

          # 時刻情報が含まれているかチェック
          has_time = Common::Utils.has_time_component?(@options[:start]) || Common::Utils.has_time_component?(@options[:end])

          objects = Common::Utils.list_s3_objects(
            @s3_client,
            @alb_config['bucket'],
            @alb_config['prefix'],
            date_prefixes
          )

          # 時刻情報が含まれている場合、時刻でフィルタリング
          if has_time
            @logger.info("日時範囲でフィルタリングします: #{start_datetime} から #{end_datetime}")
            objects = filter_objects_by_time(objects, start_datetime, end_datetime)
          end

          objects.map do |obj|
            {
              bucket: @alb_config['bucket'],
              key: obj.key,
              local: false
            }
          end
        else
          @logger.error("ファイルまたは期間が指定されていません")
          []
        end
      end

      # S3オブジェクトを時刻でフィルタリング
      def filter_objects_by_time(objects, start_datetime, end_datetime)
        filtered_objects = []

        objects.each do |obj|
          # ALBログの場合、キー名から時刻を抽出
          # 例: AWSLogs/148189048278/elasticloadbalancing/ap-northeast-1/2025/05/07/148189048278_elasticloadbalancing_ap-northeast-1_app.driver-alb-ecs-prd.5b4cd94eaad7f259_20250507T0000Z_13.112.182.157_3d8wrjtz.log.gz

          # まずパス内の年/月/日を抽出
          path_match = obj.key.match(/(\d{4})\/(\d{2})\/(\d{2})/)

          # 次にファイル名内のタイムスタンプを抽出 (YYYYMMDDTHHMMZ形式)
          timestamp_match = obj.key.match(/(\d{8})T(\d{2})(\d{2})Z/)

          if path_match && timestamp_match
            # パスから年月日を取得
            year, month, day = path_match.captures.map(&:to_i)
            # タイムスタンプから時分を取得
            hour = timestamp_match[2].to_i
            minute = timestamp_match[3].to_i
            second = 0

            # UTCとしてDateTimeを作成
            obj_time = DateTime.new(year, month, day, hour, minute, second, 0)

            @logger.debug("ファイル #{obj.key} の時刻: #{obj_time}")

            # 時刻範囲内かチェック
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

      # ファイルのダウンロードとコンテンツ取得
      def download(files)
        @logger.info("#{files.size}件のログファイルをダウンロードします")

        contents = []
        files.each do |file|
          begin
            if file[:local]
              @logger.info("ローカルファイルを読み込みます: #{file[:path]}")
              content = Common::Utils.read_local_file(file[:path])
            else
              @logger.info("S3からダウンロードします: #{file[:bucket]}/#{file[:key]}")
              content = Common::Utils.download_s3_object(
                @s3_client,
                file[:bucket],
                file[:key]
              )
            end
            contents << content
          rescue => e
            @logger.error("ファイル取得に失敗しました: #{e.message}")
            @logger.debug(e.backtrace.join("\n"))
          end
        end

        contents.join("\n")
      end
    end
  end
end
