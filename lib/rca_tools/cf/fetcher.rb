require_relative '../common/base_fetcher'

module RcaTools
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

        # 基本プレフィックスを設定 (配信元名を含むパスなど)
        base_prefix = "#{@service_config['prefix']}"
        @logger.info("基本プレフィックス: #{base_prefix}")
        @logger.info("バケット名: #{@service_config['bucket']}")

        # 日付範囲から複数の形式のファイル名パターンを生成
        date_prefixes.each do |date|
          # YYYY/MM/DD形式を分解
          parts = date.split('/')
          year, month, day = parts

          # 時間ごとのより具体的なプレフィックスを生成
          24.times do |hour|
            hour_str = hour.to_s.rjust(2, '0')
            date_str = "#{year}-#{month}-#{day}-#{hour_str}"

            # 配信元IDを使用したプレフィックスを生成
            if @service_config['distribution_id']
              # 配信元ID + 日付時間でプレフィックスを絞り込む
              specific_prefix = "#{base_prefix}#{@service_config['distribution_id']}.#{date_str}"
              @logger.info("時間別プレフィックスで検索: #{specific_prefix}")

              begin
                # より具体的なプレフィックスでオブジェクトを取得
                response = @s3_client.list_objects_v2(
                  bucket: @service_config['bucket'],
                  prefix: specific_prefix
                )

                if response.contents && !response.contents.empty?
                  @logger.info("#{specific_prefix}で#{response.contents.size}件のオブジェクトが見つかりました")
                  all_objects += response.contents
                end
              rescue => e
                @logger.error("S3オブジェクトの取得中にエラーが発生しました: #{e.message}")
                @logger.debug(e.backtrace.join("\n"))
              end
            else
              # 配信元IDが設定されていない場合は、日付時間のみでプレフィックスを生成
              # これは効率が悪いが、互換性のために残す
              specific_prefix = "#{base_prefix}"
              @logger.info("基本プレフィックスのみで検索: #{specific_prefix}")

              begin
                response = @s3_client.list_objects_v2(
                  bucket: @service_config['bucket'],
                  prefix: specific_prefix
                )

                if response.contents && !response.contents.empty?
                  @logger.info("基本プレフィックスで#{response.contents.size}件のオブジェクトが見つかりました")

                  # 日付パターンに一致するファイルをフィルタリング
                  filtered_objects = response.contents.select do |obj|
                    obj.key.include?(date_str)
                  end

                  all_objects += filtered_objects
                  @logger.info("日付「#{date_str}」でフィルタリング後: #{filtered_objects.size}件")
                end
              rescue => e
                @logger.error("S3オブジェクトの取得中にエラーが発生しました: #{e.message}")
                @logger.debug(e.backtrace.join("\n"))
              end
            end
          end
        end

        @logger.info("合計#{all_objects.size}件のファイルが見つかりました")
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

            # ファイルの時間範囲を計算
            # 開始時刻: ファイル名の時刻（例: 2025-05-22 01:00:00）
            file_start_time = obj_time
            # 終了時刻: 開始時刻 + 1時間（例: 2025-05-22 02:00:00）
            file_end_time = obj_time + Rational(1, 24)

            @logger.debug("ファイルの時間範囲: #{file_start_time} から #{file_end_time}")
            @logger.debug("指定された時間範囲: #{start_datetime} から #{end_datetime}")

            # 時間範囲の重なりをチェック
            # (ファイルの開始時刻 <= ユーザー終了時刻) かつ (ファイルの終了時刻 > ユーザー開始時刻)
            if file_start_time <= end_datetime && file_end_time > start_datetime
              @logger.debug("時間範囲が重複: #{obj.key} を含めます")
              filtered_objects << obj
            else
              @logger.debug("時間範囲が重複しない: #{obj.key} をスキップします")
            end
          else
            # 時刻が抽出できない場合はlast_modifiedを使用
            if obj.last_modified >= start_datetime && obj.last_modified <= end_datetime
              filtered_objects << obj
            end
          end
        end

        @logger.info("時間フィルタリング後のファイル数: #{filtered_objects.size}件")
        filtered_objects
      end
    end
  end
end
