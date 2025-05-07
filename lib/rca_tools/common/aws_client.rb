require 'aws-sdk-s3'
require 'yaml'

module RcaTools
  module Common
    class AwsClient
      def initialize(region:)
        # デフォルトのクレデンシャルプロバイダーチェーンを使用する
        # 環境変数(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)が優先的に使われる
        @s3 = Aws::S3::Client.new(region: region)
      end

      def s3_client
        @s3
      end
    end
  end
end
