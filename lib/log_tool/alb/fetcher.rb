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
          # 日付範囲指定モード
          @logger.info("期間指定でログを取得します: #{@options[:start]} から #{@options[:end]}")
          date_prefixes = Common::Utils.date_prefixes(@options[:start], @options[:end])
          objects = Common::Utils.list_s3_objects(
            @s3_client,
            @alb_config['bucket'],
            @alb_config['prefix'],
            date_prefixes
          )

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
