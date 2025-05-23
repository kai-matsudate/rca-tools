require_relative '../common/base_fetcher'

module RcaTools
  module Cf
    # CloudFrontログフェッチャー
    class Fetcher < Common::BaseFetcher
      # 設定の初期化
      def setup_config
        @service_config = @config['cf']
      end

      # Determine which hours should be searched
      def get_hours_to_search
        # When user specifies time range
        if @options[:start] && @options[:end] &&
           Common::Utils.has_time_component?(@options[:start]) &&
           Common::Utils.has_time_component?(@options[:end])

          # Parse start and end timestamps
          start_datetime = Common::Utils.parse_datetime(@options[:start])
          end_datetime = Common::Utils.parse_datetime(@options[:end])

          # Extract hour components from start and end times
          start_hour = start_datetime.hour
          end_hour = end_datetime.hour

          # Same date and same hour period
          if start_datetime.to_date == end_datetime.to_date && start_hour == end_hour
            @logger.info("Time optimization: Searching only for the #{start_hour} hour period")
            return [start_hour]

          # Same date but spanning multiple hour periods
          elsif start_datetime.to_date == end_datetime.to_date
            hours = (start_hour..end_hour).to_a
            @logger.info("Time optimization: Searching from hour #{start_hour} to hour #{end_hour} (#{hours.size} hour periods)")
            return hours
          end
        end

        # For unspecified time ranges or spanning multiple days, search all hour periods
        @logger.info("No time optimization: Searching all hour periods (0-23)")
        (0..23).to_a
      end

      # 日付範囲に対応するオブジェクトリストを取得する
      def list_objects_for_date_range(date_prefixes)
        all_objects = []

        # Set the base prefix (including distribution path)
        base_prefix = "#{@service_config['prefix']}"
        @logger.info("Base prefix: #{base_prefix}")
        @logger.info("Bucket name: #{@service_config['bucket']}")

        # 時間範囲が指定されている場合は、検索する時間帯を最適化
        hours_to_search = get_hours_to_search

        # 日付範囲から複数の形式のファイル名パターンを生成
        date_prefixes.each do |date|
          # YYYY/MM/DD形式を分解
          parts = date.split('/')
          year, month, day = parts

          # 時間ごとのより具体的なプレフィックスを生成
          hours_to_search.each do |hour|
            hour_str = hour.to_s.rjust(2, '0')
            date_str = "#{year}-#{month}-#{day}-#{hour_str}"

            # 配信元IDを使用したプレフィックスを生成
            if @service_config['distribution_id']
              # 配信元ID + 日付時間でプレフィックスを絞り込む
              specific_prefix = "#{base_prefix}#{@service_config['distribution_id']}.#{date_str}"
              @logger.info("Searching with hourly prefix: #{specific_prefix}")

              begin
                # Get objects with the specific prefix
                response = @s3_client.list_objects_v2(
                  bucket: @service_config['bucket'],
                  prefix: specific_prefix
                )

                if response.contents && !response.contents.empty?
                  @logger.info("Found #{response.contents.size} objects with prefix #{specific_prefix}")
                  all_objects += response.contents
                end
              rescue => e
                @logger.error("Error occurred while retrieving S3 objects: #{e.message}")
                @logger.debug(e.backtrace.join("\n"))
              end
            else
              # When distribution ID is not set, use only date-time for prefix
              # This is less efficient but kept for backward compatibility
              specific_prefix = "#{base_prefix}"
              @logger.info("Searching with base prefix only: #{specific_prefix}")

              begin
                response = @s3_client.list_objects_v2(
                  bucket: @service_config['bucket'],
                  prefix: specific_prefix
                )

                if response.contents && !response.contents.empty?
                  @logger.info("Found #{response.contents.size} objects with base prefix")

                  # Filter files matching the date pattern
                  filtered_objects = response.contents.select do |obj|
                    obj.key.include?(date_str)
                  end

                  all_objects += filtered_objects
                  @logger.info("After filtering by date '#{date_str}': #{filtered_objects.size} files")
                end
              rescue => e
                @logger.error("Error occurred while retrieving S3 objects: #{e.message}")
                @logger.debug(e.backtrace.join("\n"))
              end
            end
          end
        end

        @logger.info("Total #{all_objects.size} files found")
        all_objects
      end

      # Filter S3 objects by timestamp
      def filter_objects_by_time(objects, start_datetime, end_datetime)
        filtered_objects = []

        objects.each do |obj|
          # Extract timestamp from CloudFront log filename
          # Example: AWSLogs/148189048278/cflogs/driver-open-prd/E126FWE9F8MOZF.2022-09-28-12.2a16302d.gz

          # Match pattern domain.YYYY-MM-DD-HH.xxxxx.gz
          match = obj.key.match(/\.(\d{4})-(\d{2})-(\d{2})-(\d{2})\./)

          if match
            year, month, day, hour = match.captures.map(&:to_i)
            # Set default values for minute and second if not specified
            minute = 0
            second = 0
            # Create DateTime as UTC
            obj_time = DateTime.new(year, month, day, hour, minute, second, 0)

            @logger.debug("File #{obj.key} timestamp: #{obj_time}")

            # Calculate the time range for the file
            # Start time: Timestamp from filename (e.g., 2025-05-22 01:00:00)
            file_start_time = obj_time
            # End time: Start time + 1 hour (e.g., 2025-05-22 02:00:00)
            file_end_time = obj_time + Rational(1, 24)

            @logger.debug("File time range: #{file_start_time} to #{file_end_time}")
            @logger.debug("Specified time range: #{start_datetime} to #{end_datetime}")

            # Check for overlap between time ranges
            # Time range overlap occurs when:
            # - The start of the hour-long file period is less than or equal to the end of the user's time range
            # - AND the end of the hour-long file period is greater than the start of the user's time range
            #
            # This ensures that files are included even when the user specifies minute-level precision
            # but the files only have hour-level precision
            if file_start_time <= end_datetime && file_end_time > start_datetime
              @logger.debug("Time ranges overlap: Including #{obj.key}")
              filtered_objects << obj
            else
              @logger.debug("Time ranges do not overlap: Skipping #{obj.key}")
            end
          else
            # Use last_modified if timestamp can't be extracted from filename
            if obj.last_modified >= start_datetime && obj.last_modified <= end_datetime
              filtered_objects << obj
            end
          end
        end

        @logger.info("Number of files after time filtering: #{filtered_objects.size}")
        filtered_objects
      end
    end
  end
end
