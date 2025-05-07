require 'uri'
require_relative '../common/utils'

module LogTool
  module Cf
    class Fetcher
      def initialize(s3_client, config, options, logger)
        @s3_client = s3_client
        @config = config
        @options = options
        @logger = logger
        @cf_config = config['cf']
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

          # CloudFrontログは YYYY-MM-DD-[順序番号] 形式のファイル名を持つ
          # プレフィックスで日付部分だけを指定して検索
          all_objects = []
          date_prefixes.each do |date_prefix|
            # CloudFrontのログファイルはYYYY-MM-DD-xx.gz形式
            prefix = "#{@cf_config['prefix']}#{date_prefix}"

            @logger.info("プレフィックスでファイルを検索: #{prefix}")
            response = @s3_client.list_objects_v2(
              bucket: @cf_config['bucket'],
              prefix: prefix
            )

            if response.contents && !response.contents.empty?
              all_objects += response.contents
            end
          end

          # 時刻情報が含まれている場合、時刻でフィルタリング
          if has_time
            @logger.info("日時範囲でフィルタリングします: #{start_datetime} から #{end_datetime}")
            all_objects = filter_objects_by_time(all_objects, start_datetime, end_datetime)
          end

          @logger.info("#{all_objects.size}件のファイルが見つかりました")

          all_objects.map do |obj|
            {
              bucket: @cf_config['bucket'],
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
          # CloudFrontログの場合、キー名から時刻を抽出
          # 例: AWSLogs/148189048278/cflogs/driver-open-prd/E126FWE9F8MOZF.2022-09-28-12.2a16302d.gz

          # 正規表現を修正してドメイン名.YYYY-MM-DD-HH.xxxxx.gzの形式に対応する
          match = obj.key.match(/\.(\d{4})-(\d{2})-(\d{2})-(\d{2})\./)

          if match
            year, month, day, hour = match.captures.map(&:to_i)
            # 時、分、秒が指定されていない場合は、デフォルト値を設定
            minute = 0
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
