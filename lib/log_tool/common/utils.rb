require 'date'
require 'fileutils'
require 'tempfile'
require 'zlib'

module LogTool
  module Common
    # ユーティリティメソッドを提供するクラス
    #
    # このクラスは、ログ処理に関連する共通のユーティリティ機能を提供します。
    # 日付処理、S3操作、ファイル操作など、複数のモジュールで使用される
    # 便利な機能が含まれています。
    class Utils
      # 日付時刻関連のメソッド

      # S3オブジェクトを日付範囲でフィルタリングするためのプレフィックスリストを生成
      # @param start_date [String, Date, DateTime] 開始日または日時
      # @param end_date [String, Date, DateTime] 終了日または日時
      # @param format [String] 日付フォーマット
      # @return [Array] プレフィックスリスト、開始日時、終了日時の配列
      def self.date_prefixes(start_date, end_date, format = '%Y/%m/%d')
        start_datetime = parse_datetime(start_date)
        end_datetime = parse_datetime(end_date)

        # 日付部分でのプレフィックス生成
        date_range = (start_datetime.to_date..end_datetime.to_date).map do |date|
          date.strftime(format)
        end

        # 返り値は [プレフィックスリスト, 開始日時, 終了日時] の形式
        [date_range, start_datetime, end_datetime]
      end

      # 日付または日時文字列をパースするヘルパーメソッド
      # @param datetime_str [String, Date, DateTime] パースする日付または日時
      # @return [DateTime] パースされたDateTimeオブジェクト
      def self.parse_datetime(datetime_str)
        if datetime_str.is_a?(String) && (datetime_str.include?('T') || datetime_str.include?(' '))
          # ISO8601形式（YYYY-MM-DDThh:mm:ss）または YYYY-MM-DD hh:mm:ss 形式
          # UTCとして扱う
          DateTime.parse(datetime_str).new_offset(0)
        elsif datetime_str.is_a?(String)
          # YYYY-MM-DD 形式
          # 日付のみの場合は、その日の始まり（00:00:00 UTC）として扱う
          date = Date.parse(datetime_str)
          DateTime.new(date.year, date.month, date.day, 0, 0, 0, 0)
        elsif datetime_str.is_a?(Date) && !datetime_str.is_a?(DateTime)
          # DateオブジェクトをDateTimeに変換
          DateTime.new(datetime_str.year, datetime_str.month, datetime_str.day, 0, 0, 0, 0)
        else
          # すでにDateTimeオブジェクトかその他の場合はそのまま返す
          datetime_str
        end
      end

      # 文字列に時刻情報が含まれているかをチェック
      # @param datetime_str [String] チェックする日付または日時文字列
      # @return [Boolean] 時刻情報が含まれている場合はtrue
      def self.has_time_component?(datetime_str)
        datetime_str.is_a?(String) && (datetime_str.include?('T') || datetime_str.include?(' '))
      end

      # S3関連のメソッド

      # S3オブジェクトリストを取得
      # @param s3_client [Aws::S3::Client] S3クライアント
      # @param bucket [String] バケット名
      # @param prefix [String] プレフィックス
      # @param date_prefixes [Array<String>] 日付プレフィックスの配列
      # @return [Array<Aws::S3::Types::Object>] S3オブジェクトの配列
      def self.list_s3_objects(s3_client, bucket, prefix, date_prefixes = nil)
        if date_prefixes
          objects = []
          date_prefixes.each do |date_prefix|
            full_prefix = "#{prefix}#{date_prefix}"
            response = s3_client.list_objects_v2(bucket: bucket, prefix: full_prefix)
            objects += response.contents if response.contents
          end
          objects
        else
          response = s3_client.list_objects_v2(bucket: bucket, prefix: prefix)
          response.contents || []
        end
      end

      # S3オブジェクトをダウンロードして内容を返す
      # @param s3_client [Aws::S3::Client] S3クライアント
      # @param bucket [String] バケット名
      # @param key [String] オブジェクトキー
      # @param decompress [Boolean] gzipファイルを解凍するかどうか
      # @return [String] ダウンロードしたオブジェクトの内容
      def self.download_s3_object(s3_client, bucket, key, decompress: true)
        temp_file = Tempfile.new('s3_object')
        begin
          s3_client.get_object(
            bucket: bucket,
            key: key,
            response_target: temp_file.path
          )

          if decompress && key.end_with?('.gz')
            content = ''
            Zlib::GzipReader.open(temp_file.path) do |gz|
              content = gz.read
            end
            content
          else
            File.read(temp_file.path)
          end
        ensure
          temp_file.close
          temp_file.unlink
        end
      end

      # ファイル操作関連のメソッド

      # ローカルファイルからコンテンツを読み込む
      # @param file_path [String] ファイルパス
      # @param decompress [Boolean] gzipファイルを解凍するかどうか
      # @return [String] ファイルの内容
      def self.read_local_file(file_path, decompress: true)
        if decompress && file_path.end_with?('.gz')
          content = ''
          Zlib::GzipReader.open(file_path) do |gz|
            content = gz.read
          end
          content
        else
          File.read(file_path)
        end
      end

      # 出力ディレクトリが存在することを確認
      # @param dir_path [String] ディレクトリパス
      def self.ensure_output_dir(dir_path)
        FileUtils.mkdir_p(dir_path) unless Dir.exist?(dir_path)
      end
    end
  end
end
