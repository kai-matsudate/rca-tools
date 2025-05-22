require 'dotenv'

module RcaTools
  module Common
    class Config
      def self.load
        # .envファイルを読み込む
        Dotenv.load

        {
          'default' => {
            'region' => ENV['DEFAULT_REGION'] || 'us-east-1',
            'output_dir' => ENV['OUTPUT_DIR'] || './output'
          },
          'alb' => {
            'bucket' => ENV['ALB_BUCKET'],
            'prefix' => ENV['ALB_PREFIX']
          },
          'cf' => {
            'bucket' => ENV['CF_BUCKET'],
            'prefix' => ENV['CF_PREFIX'],
            'distribution_id' => ENV['CF_DISTRIBUTION_ID']
          },
          'waf' => {
            's3_bucket' => ENV['WAF_S3_BUCKET'],
            's3_prefix' => ENV['WAF_S3_PREFIX']
          }
        }
      end
    end
  end
end
