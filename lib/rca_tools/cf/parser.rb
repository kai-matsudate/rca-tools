require 'csv'
require 'fileutils'

module RcaTools
  module Cf
    class Parser
      # CloudFront log header definition
      # Reference: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#BasicDistributionFileFormat
      HEADERS = [
        'date',
        'time',
        'x-edge-location',
        'sc-bytes',
        'c-ip',
        'cs-method',
        'cs-host',
        'cs-uri-stem',
        'sc-status',
        'cs-referer',
        'cs-user-agent',
        'cs-uri-query',
        'cs-cookie',
        'x-edge-result-type',
        'x-edge-request-id',
        'x-host-header',
        'cs-protocol',
        'cs-bytes',
        'time-taken',
        'x-forwarded-for',
        'ssl-protocol',
        'ssl-cipher',
        'x-edge-response-result-type',
        'cs-protocol-version',
        'fle-status',
        'fle-encrypted-fields',
        'c-port',
        'time-to-first-byte',
        'x-edge-detailed-result-type',
        'sc-content-type',
        'sc-content-len',
        'sc-range-start',
        'sc-range-end'
      ]

      def initialize(content, logger)
        @raw_content = content
        @logger = logger
      end

      # Convert logs to CSV
      def to_csv(output_path)
        ensure_output_dir(File.dirname(output_path))

        parsed_count = 0
        error_count = 0

        CSV.open(output_path, 'w', headers: HEADERS, write_headers: true) do |csv|
          @raw_content.each_line do |line|
            line = line.strip
            next if line.empty?

            # Skip comment lines at the beginning of CloudFront logs
            next if line.start_with?('#')

            begin
              # CloudFront logs are tab-delimited
              fields = line.split("\t")
              csv << fields
              parsed_count += 1
            rescue => e
              @logger.warn("Failed to parse log line: #{e.message}")
              @logger.debug("Problematic line: #{line}")
              error_count += 1
            end
          end
        end

        @logger.info("Processing complete: success=#{parsed_count}, failed=#{error_count}")
        parsed_count
      end

      private

      def ensure_output_dir(dir)
        FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
      end
    end
  end
end
