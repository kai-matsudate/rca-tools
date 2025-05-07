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
      # @param logger [Logger] ロガーオブジェクト
      # @return [String] ダウンロードしたオブジェクトの内容
      def self.download_s3_object(s3_client, bucket, key, decompress: true, logger: nil)
        temp_file = Tempfile.new('s3_object')
        begin
          # S3からオブジェクトをダウンロード
          logger&.info("S3オブジェクトのダウンロード開始: #{bucket}/#{key}")
          response = s3_client.get_object(
            bucket: bucket,
            key: key,
            response_target: temp_file.path
          )

          # ファイルの内容を取得
          if decompress && key.end_with?('.gz')
            # 外部コマンドを使用してgzipファイルを解凍
            content = decompress_with_external_command(temp_file.path, logger)

            if content.nil? || content.empty?
              # 最後の手段としてRubyのZlib解凍を試みる
              logger&.info("代替手段としてRubyのZlib解凍を試みます")
              begin
                content = ''
                Zlib::GzipReader.open(temp_file.path) do |gz|
                  content = gz.read
                end
              rescue => e
                logger&.error("Zlib解凍エラー: #{e.message}")
                content = nil
              end
            end

            content
          else
            # 非圧縮ファイルの読み込み
            File.read(temp_file.path)
          end
        ensure
          temp_file.close
          temp_file.unlink
        end
      end

      # 外部コマンドを使用してGZIPファイルを解凍
      # @param gz_file_path [String] GZIPファイルのパス
      # @param logger [Logger] ロガーオブジェクト
      # @return [String, nil] 解凍したコンテンツ、または失敗した場合はnil
      def self.decompress_with_external_command(gz_file_path, logger)
        begin
          output_path = gz_file_path + '.decoded'

          # まずgunzipコマンドを試す
          command = "gunzip -c '#{gz_file_path}' > '#{output_path}'"
          result = system(command)

          if result && File.exist?(output_path) && File.size(output_path) > 0
            # 成功した場合はファイルを読み込む
            content = File.read(output_path)
          else
            # gunzipが失敗した場合は、macOSのditto -k コマンドを試す (macOSのみ)
            ditto_temp_dir = File.join(Dir.pwd, 'output', 'ditto_temp')
            FileUtils.mkdir_p(ditto_temp_dir)

            # dittoコマンドは解凍先にディレクトリが必要
            ditto_command = "ditto -k --sequesterRsrc '#{gz_file_path}' '#{ditto_temp_dir}'"
            ditto_result = system(ditto_command)

            if ditto_result
              # 解凍されたファイルを探す (拡張子なしのファイル名になる)
              base_name = File.basename(gz_file_path, '.gz')
              extracted_file = File.join(ditto_temp_dir, base_name)

              if File.exist?(extracted_file)
                content = File.read(extracted_file)

                # 結果を同じ出力パスにコピー
                File.write(output_path, content)
              else
                return nil
              end
            else
              return nil
            end
          end

          # 解凍結果を返す前に一時ファイルの削除
          begin
            File.unlink(output_path) if File.exist?(output_path)
            FileUtils.rm_rf(File.join(Dir.pwd, 'output', 'ditto_temp')) if Dir.exist?(File.join(Dir.pwd, 'output', 'ditto_temp'))
          rescue => e
            # 一時ファイル削除中のエラーは無視
          end

          content
        rescue => e
          logger&.error("外部コマンドでの解凍エラー: #{e.message}")
          nil
        end
      end

      # ファイル操作関連のメソッド

      # ローカルファイルからコンテンツを読み込む
      # @param file_path [String] ファイルパス
      # @param decompress [Boolean] gzipファイルを解凍するかどうか
      # @param logger [Logger] ロガーオブジェクト (オプション)
      # @return [String] ファイルの内容
      def self.read_local_file(file_path, decompress: true, logger: nil)
        if decompress && file_path.end_with?('.gz')
          # 外部コマンドでの解凍を優先
          content = decompress_with_external_command(file_path, logger)

          # 外部コマンドが失敗した場合のみ、Zlibを試用
          if content.nil? || content.empty?
            begin
              content = ''
              Zlib::GzipReader.open(file_path) do |gz|
                content = gz.read
              end
            rescue => e
              logger&.error("Zlib解凍エラー: #{e.message}")
              raise
            end
          end

          content
        else
          # 非圧縮ファイルの読み込み
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
