require 'spec_helper'
require_relative '../../lib/log_tool/alb/parser'
require_relative '../../lib/log_tool/common/logger'
require 'fileutils'
require 'csv'

RSpec.describe LogTool::Alb::Parser do
  let(:logger) { LogTool::Common::AppLogger.build }
  let(:sample_log_path) { File.join(File.dirname(__FILE__), '../fixtures/sample_alb_log.txt') }
  let(:output_path) { File.join(File.dirname(__FILE__), '../tmp/alb_output.csv') }

  before do
    # テスト出力ディレクトリを作成
    FileUtils.mkdir_p(File.dirname(output_path))
  end

  after do
    # テスト後にファイルをクリーンアップ
    FileUtils.rm_f(output_path)
  end

  describe '#to_csv' do
    it 'ALBログをCSVに変換する' do
      # サンプルALBログファイルを読み込む
      content = File.read(sample_log_path)
      parser = described_class.new(content, logger)

      # CSVに変換
      count = parser.to_csv(output_path)

      # 変換結果を検証
      expect(count).to eq(2) # 2行のログが処理されたはず
      expect(File.exist?(output_path)).to be true

      # CSVの内容を検証
      csv_content = CSV.read(output_path, headers: true)
      expect(csv_content.size).to eq(2)

      # 最初の行の内容を検証
      first_row = csv_content[0]
      expect(first_row['type']).to eq('http')
      expect(first_row['timestamp']).to eq('2025-05-01T10:15:30.123456Z')
      expect(first_row['elb_status_code']).to eq('200')
      expect(first_row['request']).to include('GET')

      # 2行目の内容を検証
      second_row = csv_content[1]
      expect(second_row['type']).to eq('http')
      expect(second_row['timestamp']).to eq('2025-05-01T10:15:31.123456Z')
      expect(second_row['elb_status_code']).to eq('404')
      expect(second_row['request']).to include('POST')
    end
  end
end
