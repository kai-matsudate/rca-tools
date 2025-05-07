require 'aws-sdk-s3'
require 'yaml'

module LogTool
  module Common
    class AwsClient
      def initialize(profile: nil, region:)
        credentials = profile ? Aws::SharedCredentials.new(profile_name: profile) : nil
        @s3 = Aws::S3::Client.new(region: region, credentials: credentials)
      end

      def s3_client
        @s3
      end
    end
  end
end
