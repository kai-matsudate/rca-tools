require 'aws-sdk-s3'
require 'tempfile'
require 'date'
require 'pathname'

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

        # 日付範囲の設定
        start_date = @options[:start] ? Date.parse(@options[:start]) : Date.today - 1
        end_date = @options[:end] ? Date.parse(@options[:end]) : Date.today

        @logger.info("期間 #{start_date} 〜 #{end_date} のログを検索")

        # S3バケットからのログファイル一覧取得
        files = []
        (start_date..end_date).each do |date|
          prefix = date_prefix(date)
          @logger.debug("S3バケット #{@bucket} のプレフィックス #{prefix} を検索")

          begin
            # S3オブジェクトリスト取得
            @s3_client.list_objects_v2(bucket: @bucket, prefix: prefix).contents.each do |object|
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

      # 日付からS3プレフィックスを生成
      def date_prefix(date)
        formatted_date = date.strftime('%Y/%m/%d')
        "#{@prefix}#{formatted_date}/"
      end
    end
  end
end
