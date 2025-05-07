require 'date'
require 'fileutils'
require 'tempfile'
require 'zlib'

module LogTool
  module Common
    class Utils
      # S3オブジェクトを日付範囲でフィルタリングするためのプレフィックスリストを生成
      def self.date_prefixes(start_date, end_date, format = '%Y-%m-%d')
        start_date = Date.parse(start_date) if start_date.is_a?(String)
        end_date = Date.parse(end_date) if end_date.is_a?(String)

        (start_date..end_date).map do |date|
          date.strftime(format)
        end
      end

      # S3オブジェクトリストを取得
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

      # ローカルファイルからコンテンツを読み込む
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
      def self.ensure_output_dir(dir_path)
        FileUtils.mkdir_p(dir_path) unless Dir.exist?(dir_path)
      end
    end
  end
end
