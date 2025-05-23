require 'thor'
require 'base64'
require 'securerandom'
require_relative '../common/aws_client'
require_relative '../common/logger'
require_relative '../common/utils'
require_relative '../common/config'
require_relative 'fetcher'
require_relative 'parser'

module RcaTools
  module Cf
    class CLI < Thor
      default_task :fetch
      desc 'fetch', 'Retrieve CloudFront logs and convert to CSV'
      option :file,   type: :string,  desc: 'S3 or local log file'
      option :start,  type: :string,  desc: 'Start datetime (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss [UTC])'
      option :end,    type: :string,  desc: 'End datetime (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss [UTC])'
      option :output, type: :string,  default: 'cf_logs.csv', desc: 'Output CSV filename'
      option :region, type: :string,  desc: 'AWS Region (defaults to .env setting)'

      def fetch
        logger = Common::AppLogger.build

        begin
          # Load configuration
          config = load_config

          # Initialize AWS client
          region = options[:region] || config['default']['region']
          client = Common::AwsClient.new(
            region: region
          ).s3_client

          # Retrieve log files
          fetcher = Fetcher.new(client, config, options, logger)
          files = fetcher.list_files

          if files.empty?
            logger.error("No target log files found")
            return
          end

          logger.info("Found #{files.size} log files")

          # Download and parse log files
          raw_logs = fetcher.download(files)

          # CSV output
          ensure_output_dir(config['default']['output_dir'])

          # Determine output filename
          output_filename =
            if options[:output] == 'cf_logs.csv' # Default value
              generate_default_filename
            else
              options[:output]
            end

          output_path = File.join(config['default']['output_dir'], output_filename)

          parser = Parser.new(raw_logs, logger)
          parsed_count = parser.to_csv(output_path)

          logger.info("Exported #{parsed_count} log entries to CSV: #{output_path}")
        rescue => e
          logger.error("An error occurred: #{e.message}")
          logger.debug(e.backtrace.join("\n"))
          exit 1
        end
      end

      private

      def load_config
        Common::Config.load
      end

      def ensure_output_dir(dir)
        Common::Utils.ensure_output_dir(dir)
      end

      # Generate default filename using Base64 encoding
      def generate_default_filename
        # Use current UTC timestamp as a unique identifier
        timestamp = Time.now.utc.strftime('%Y%m%d%H%M%S')
        # Add random element to enhance uniqueness
        random_suffix = SecureRandom.hex(4)
        identifier = "#{timestamp}_#{random_suffix}"
        # Base64 encode the identifier
        encoded = Base64.urlsafe_encode64(identifier, padding: false)
        "cf_#{encoded}.csv"
      end
    end
  end
end
