require 'uri'
require_relative 'utils'

module LogTool
  module Common
    # ログフェッチャーの基本クラス
    # 各サービス固有のフェッチャーはこのクラスを継承する
    class BaseFetcher
      attr_reader :s3_client, :config, :options, :logger, :service_config

      def initialize(s3_client, config, options, logger)
        @s3_client = s3_client
        @config = config
        @options = options
        @logger = logger
        setup_config
      end

      # サブクラスで実装する設定メソッド
      def setup_config
        raise NotImplementedError, "#{self.class} must implement #setup_config"
      end

      # ファイルリストの取得
      def list_files
        if single_file_mode?
          handle_single_file
        elsif date_range_mode?
          handle_date_range
        else
          @logger.error("ファイルまたは期間が指定されていません")
          []
        end
      end

      # 単一ファイルが指定されているかどうか
      def single_file_mode?
        @options[:file]
      end

      # 日付/日時範囲が指定されているかどうか
      def date_range_mode?
        @options[:start] && @options[:end]
      end

      # 単一ファイルの処理
      def handle_single_file
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
      end

      # 日付/日時範囲の処理
      def handle_date_range
        @logger.info("期間指定でログを取得します: #{@options[:start]} から #{@options[:end]}")

        # 日付プレフィックスと日時情報を取得
        date_prefixes, start_datetime, end_datetime = Utils.date_prefixes(@options[:start], @options[:end])

        # 時刻情報が含まれているかチェック
        has_time = Utils.has_time_component?(@options[:start]) || Utils.has_time_component?(@options[:end])

        # オブジェクトリスト取得 (サブクラスで実装)
        objects = list_objects_for_date_range(date_prefixes)

        # 時刻情報が含まれている場合、時刻でフィルタリング
        if has_time
          @logger.info("日時範囲でフィルタリングします: #{start_datetime} から #{end_datetime}")
          objects = filter_objects_by_time(objects, start_datetime, end_datetime)
        end

        format_object_list(objects)
      end

      # 日付範囲に対応するオブジェクトリストを取得する (サブクラスで実装)
      def list_objects_for_date_range(date_prefixes)
        raise NotImplementedError, "#{self.class} must implement #list_objects_for_date_range"
      end

      # S3オブジェクトを時刻でフィルタリング (サブクラスで実装)
      def filter_objects_by_time(objects, start_datetime, end_datetime)
        raise NotImplementedError, "#{self.class} must implement #filter_objects_by_time"
      end

      # オブジェクトリストを標準形式に変換 (サブクラスでオーバーライド可能)
      def format_object_list(objects)
        objects.map do |obj|
          {
            bucket: @service_config['bucket'],
            key: obj.key,
            local: false
          }
        end
      end

      # ファイルのダウンロードとコンテンツ取得
      def download(files)
        @logger.info("#{files.size}件のログファイルをダウンロードします")
        collect_contents(files)
      end

      # ファイルからコンテンツを収集
      def collect_contents(files)
        contents = []
        files.each do |file|
          begin
            content = download_file(file)
            contents << content if content
          rescue => e
            handle_download_error(file, e)
          end
        end

        contents.join("\n")
      end

      # ファイルをダウンロード
      def download_file(file)
        if file[:local]
          @logger.info("ローカルファイルを読み込みます: #{file[:path]}")
          Utils.read_local_file(file[:path], logger: @logger)
        else
          @logger.info("S3からダウンロードします: #{file[:bucket]}/#{file[:key]}")
          Utils.download_s3_object(
            @s3_client,
            file[:bucket],
            file[:key],
            logger: @logger # ロガーオブジェクトを渡す
          )
        end
      end

      # ダウンロードエラーの処理
      def handle_download_error(file, error)
        @logger.error("ファイル取得に失敗しました: #{error.message}")
        @logger.debug(error.backtrace.join("\n"))
      end
    end
  end
end
