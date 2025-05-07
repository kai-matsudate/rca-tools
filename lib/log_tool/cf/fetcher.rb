require_relative '../common/base_fetcher'

module LogTool
  module Cf
    # CloudFrontログフェッチャー
    class Fetcher < Common::BaseFetcher
      # 設定の初期化
      def setup_config
        @service_config = @config['cf']
      end

      # 日付範囲に対応するオブジェクトリストを取得する
      def list_objects_for_date_range(date_prefixes)
        all_objects = []

        date_prefixes.each do |date_prefix|
          # CloudFrontのログファイルはYYYY-MM-DD-xx.gz形式
          prefix = "#{@service_config['prefix']}#{date_prefix}"

          @logger.info("プレフィックスでファイルを検索: #{prefix}")
          response = @s3_client.list_objects_v2(
            bucket: @service_config['bucket'],
            prefix: prefix
          )

          if response.contents && !response.contents.empty?
            all_objects += response.contents
          end
        end

        @logger.info("#{all_objects.size}件のファイルが見つかりました")
        all_objects
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
    end
  end
end
