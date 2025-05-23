require 'date'
require 'fileutils'
require 'tempfile'
require 'zlib'

module RcaTools
  module Common
    # Utility class providing common methods
    #
    # This class provides common utility functions related to log processing.
    # It includes useful features such as date handling, S3 operations,
    # file operations, etc. used across multiple modules.
    class Utils
      # Date and time related methods

      # Generate prefix list for filtering S3 objects by date range
      # @param start_date [String, Date, DateTime] Start date or datetime
      # @param end_date [String, Date, DateTime] End date or datetime
      # @param format [String] Date format
      # @return [Array] Array containing prefix list, start datetime, and end datetime
      def self.date_prefixes(start_date, end_date, format = '%Y/%m/%d')
        start_datetime = parse_datetime(start_date)
        end_datetime = parse_datetime(end_date)

        # Generate prefixes for date portions
        date_range = (start_datetime.to_date..end_datetime.to_date).map do |date|
          date.strftime(format)
        end

        # Return value is in the format [prefix_list, start_datetime, end_datetime]
        [date_range, start_datetime, end_datetime]
      end

      # Helper method for parsing date or datetime strings
      # @param datetime_str [String, Date, DateTime] Date or datetime to parse
      # @return [DateTime] Parsed DateTime object
      def self.parse_datetime(datetime_str)
        if datetime_str.is_a?(String) && (datetime_str.include?('T') || datetime_str.include?(' '))
          # ISO8601 format (YYYY-MM-DDThh:mm:ss) or YYYY-MM-DD hh:mm:ss format
          # Treated as UTC
          DateTime.parse(datetime_str).new_offset(0)
        elsif datetime_str.is_a?(String)
          # YYYY-MM-DD format
          # If date only, treat as the beginning of the day (00:00:00 UTC)
          date = Date.parse(datetime_str)
          DateTime.new(date.year, date.month, date.day, 0, 0, 0, 0)
        elsif datetime_str.is_a?(Date) && !datetime_str.is_a?(DateTime)
          # Convert Date object to DateTime
          DateTime.new(datetime_str.year, datetime_str.month, datetime_str.day, 0, 0, 0, 0)
        else
          # If already a DateTime object or other, return as is
          datetime_str
        end
      end

      # Check if a string contains time information
      # @param datetime_str [String] Date or datetime string to check
      # @return [Boolean] true if the string contains time information
      def self.has_time_component?(datetime_str)
        datetime_str.is_a?(String) && (datetime_str.include?('T') || datetime_str.include?(' '))
      end

      # S3 related methods

      # Get S3 object list
      # @param s3_client [Aws::S3::Client] S3 client
      # @param bucket [String] Bucket name
      # @param prefix [String] Prefix
      # @param date_prefixes [Array<String>] Array of date prefixes
      # @return [Array<Aws::S3::Types::Object>] Array of S3 objects
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

      # Download S3 object and return its content
      # @param s3_client [Aws::S3::Client] S3 client
      # @param bucket [String] Bucket name
      # @param key [String] Object key
      # @param decompress [Boolean] Whether to decompress gzip files
      # @param logger [Logger] Logger object
      # @return [String] Content of the downloaded object
      def self.download_s3_object(s3_client, bucket, key, decompress: true, logger: nil)
        temp_file = Tempfile.new('s3_object')
        begin
          # Download object from S3
          logger&.info("Starting download of S3 object: #{bucket}/#{key}")
          response = s3_client.get_object(
            bucket: bucket,
            key: key,
            response_target: temp_file.path
          )

          # Get file content
          if decompress && key.end_with?('.gz')
            # Use external command to decompress gzip file
            content = decompress_with_external_command(temp_file.path, logger)

            if content.nil? || content.empty?
              # Try Ruby's Zlib decompression as a last resort
              logger&.info("Trying Ruby's Zlib decompression as an alternative")
              begin
                content = ''
                Zlib::GzipReader.open(temp_file.path) do |gz|
                  content = gz.read
                end
              rescue => e
                logger&.error("Zlib decompression error: #{e.message}")
                content = nil
              end
            end

            content
          else
            # Read non-compressed file
            File.read(temp_file.path)
          end
        ensure
          temp_file.close
          temp_file.unlink
        end
      end

      # Decompress GZIP file using external command
      # @param gz_file_path [String] Path to GZIP file
      # @param logger [Logger] Logger object
      # @return [String, nil] Decompressed content, or nil if failed
      def self.decompress_with_external_command(gz_file_path, logger)
        begin
          output_path = gz_file_path + '.decoded'

          # First try the gunzip command
          command = "gunzip -c '#{gz_file_path}' > '#{output_path}'"
          result = system(command)

          if result && File.exist?(output_path) && File.size(output_path) > 0
            # If successful, read the file
            content = File.read(output_path)
          else
            # If gunzip fails, try ditto -k command (macOS only)
            ditto_temp_dir = File.join(Dir.pwd, 'output', 'ditto_temp')
            FileUtils.mkdir_p(ditto_temp_dir)

            # ditto command requires a directory for extraction
            ditto_command = "ditto -k --sequesterRsrc '#{gz_file_path}' '#{ditto_temp_dir}'"
            ditto_result = system(ditto_command)

            if ditto_result
              # Look for extracted file (will have filename without extension)
              base_name = File.basename(gz_file_path, '.gz')
              extracted_file = File.join(ditto_temp_dir, base_name)

              if File.exist?(extracted_file)
                content = File.read(extracted_file)

                # Copy result to the same output path
                File.write(output_path, content)
              else
                return nil
              end
            else
              return nil
            end
          end

          # Clean up temporary files before returning decompression result
          begin
            File.unlink(output_path) if File.exist?(output_path)
            FileUtils.rm_rf(File.join(Dir.pwd, 'output', 'ditto_temp')) if Dir.exist?(File.join(Dir.pwd, 'output', 'ditto_temp'))
          rescue => e
            # Ignore errors during temporary file cleanup
          end

          content
        rescue => e
          logger&.error("Error decompressing with external command: #{e.message}")
          nil
        end
      end

      # File operation related methods

      # Read content from local file
      # @param file_path [String] File path
      # @param decompress [Boolean] Whether to decompress gzip files
      # @param logger [Logger] Logger object (optional)
      # @return [String] File content
      def self.read_local_file(file_path, decompress: true, logger: nil)
        if decompress && file_path.end_with?('.gz')
          # Prioritize external command for decompression
          content = decompress_with_external_command(file_path, logger)

          # Only try Zlib if external command fails
          if content.nil? || content.empty?
            begin
              content = ''
              Zlib::GzipReader.open(file_path) do |gz|
                content = gz.read
              end
            rescue => e
              logger&.error("Zlib decompression error: #{e.message}")
              raise
            end
          end

          content
        else
          # Read non-compressed file
          File.read(file_path)
        end
      end

      # Ensure output directory exists
      # @param dir_path [String] Directory path
      def self.ensure_output_dir(dir_path)
        FileUtils.mkdir_p(dir_path) unless Dir.exist?(dir_path)
      end
    end
  end
end
