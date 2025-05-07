require 'logger'
require 'time'

module LogTool
  module Common
    class AppLogger
      def self.build
        logger = Logger.new($stdout)
        logger.level = Logger::INFO
        logger.formatter = proc { |sev, datetime, _, msg|
          "[#{datetime.strftime('%Y-%m-%dT%H:%M:%S%z')}] #{sev}: #{msg}\n"
        }
        logger
      end
    end
  end
end
