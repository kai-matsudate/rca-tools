require_relative '../common/base_fetcher'

module LogTool
  module Alb
    # ALBログフェッチャー
    class Fetcher < Common::BaseFetcher
      # 設定の初期化
      def setup_config
        @service_config = @config['alb']
      end

      # 日付範囲に対応するオブジェクトリストを取得する
      def list_objects_for_date_range(date_prefixes)
        Common::Utils.list_s3_objects(
          @s3_client,
          @service_config['bucket'],
          @service_config['prefix'],
          date_prefixes
        )
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
    end
  end
end
