require 'uri'
require_relative 'utils'

module RcaTools
  module Common
    # Base class for log fetchers
    # Service-specific fetchers inherit from this class
    class BaseFetcher
      attr_reader :s3_client, :config, :options, :logger, :service_config

      def initialize(s3_client, config, options, logger)
        @s3_client = s3_client
        @config = config
        @options = options
        @logger = logger
        setup_config
      end

      # Configuration method to be implemented by subclasses
      def setup_config
        raise NotImplementedError, "#{self.class} must implement #setup_config"
      end

      # Get list of files
      def list_files
        if single_file_mode?
          handle_single_file
        elsif date_range_mode?
          handle_date_range
        else
          @logger.error("No file or date range specified")
          []
        end
      end

      # Check if a single file is specified
      def single_file_mode?
        @options[:file]
      end

      # Check if a date/time range is specified
      def date_range_mode?
        @options[:start] && @options[:end]
      end

      # Process a single file
      def handle_single_file
        file_uri = URI.parse(@options[:file])
        if file_uri.scheme == 's3'
          @logger.info("Retrieving single file from S3: #{@options[:file]}")
          # Extract bucket and key from s3://bucket/path/to/file.gz format
          bucket = file_uri.host
          key = file_uri.path.sub(/^\//, '')
          [{
            bucket: bucket,
            key: key,
            local: false
          }]
        else
          @logger.info("Processing local file: #{@options[:file]}")
          # Local file
          [{
            path: @options[:file],
            local: true
          }]
        end
      end

      # Process date/time range
      def handle_date_range
        @logger.info("Retrieving logs for date range: #{@options[:start]} to #{@options[:end]}")

        # Get date prefixes and datetime information
        date_prefixes, start_datetime, end_datetime = Utils.date_prefixes(@options[:start], @options[:end])

        # Check if time component is included
        has_time = Utils.has_time_component?(@options[:start]) || Utils.has_time_component?(@options[:end])

        # Get object list (implemented by subclasses)
        objects = list_objects_for_date_range(date_prefixes)

        # Filter by time if time component is specified
        if has_time
          @logger.info("Filtering by datetime range: #{start_datetime} to #{end_datetime}")
          objects = filter_objects_by_time(objects, start_datetime, end_datetime)
        end

        format_object_list(objects)
      end

      # Get object list for date range (to be implemented by subclasses)
      def list_objects_for_date_range(date_prefixes)
        raise NotImplementedError, "#{self.class} must implement #list_objects_for_date_range"
      end

      # Filter S3 objects by time (to be implemented by subclasses)
      def filter_objects_by_time(objects, start_datetime, end_datetime)
        raise NotImplementedError, "#{self.class} must implement #filter_objects_by_time"
      end

      # Convert object list to standard format (can be overridden by subclasses)
      def format_object_list(objects)
        objects.map do |obj|
          {
            bucket: @service_config['bucket'],
            key: obj.key,
            local: false
          }
        end
      end

      # Download files and get content
      def download(files)
        @logger.info("Downloading #{files.size} log files")
        collect_contents(files)
      end

      # Collect content from files
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

      # Download file
      def download_file(file)
        if file[:local]
          @logger.info("Reading local file: #{file[:path]}")
          Utils.read_local_file(file[:path], logger: @logger)
        else
          @logger.info("Downloading from S3: #{file[:bucket]}/#{file[:key]}")
          Utils.download_s3_object(
            @s3_client,
            file[:bucket],
            file[:key],
            logger: @logger # Pass logger object
          )
        end
      end

      # Handle download error
      def handle_download_error(file, error)
        @logger.error("Failed to retrieve file: #{error.message}")
        @logger.debug(error.backtrace.join("\n"))
      end
    end
  end
end
