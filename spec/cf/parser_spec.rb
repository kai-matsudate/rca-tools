require 'spec_helper'
require_relative '../../lib/rca_tools/cf/parser'
require_relative '../../lib/rca_tools/common/logger'
require 'fileutils'
require 'csv'

RSpec.describe RcaTools::Cf::Parser do
  let(:logger) { RcaTools::Common::AppLogger.build }
  let(:sample_log_path) { File.join(File.dirname(__FILE__), '../fixtures/sample_cf_log.txt') }
  let(:output_path) { File.join(File.dirname(__FILE__), '../tmp/cf_output.csv') }

  before do
    # テスト出力ディレクトリを作成
    FileUtils.mkdir_p(File.dirname(output_path))
  end

  after do
    # テスト後にファイルをクリーンアップ
    FileUtils.rm_f(output_path)
  end

  describe '#to_csv' do
    it 'CloudFrontログをCSVに変換する' do
      # サンプルCloudFrontログファイルを読み込む
      content = File.read(sample_log_path)
      parser = described_class.new(content, logger)

      # CSVに変換
      count = parser.to_csv(output_path)

      # 変換結果を検証
      expect(count).to eq(2) # 2行のログが処理されたはず（コメント行は除外）
      expect(File.exist?(output_path)).to be true

      # CSVの内容を検証
      csv_content = CSV.read(output_path, headers: true)
      expect(csv_content.size).to eq(2)

      # 最初の行の内容を検証
      first_row = csv_content[0]
      expect(first_row['date']).to eq('2025-05-01')
      expect(first_row['time']).to eq('10:15:30')
      expect(first_row['sc-status']).to eq('200')
      expect(first_row['cs-method']).to eq('GET')
      expect(first_row['cs-uri-stem']).to eq('/index.html')

      # 2行目の内容を検証
      second_row = csv_content[1]
      expect(second_row['date']).to eq('2025-05-01')
      expect(second_row['time']).to eq('10:15:31')
      expect(second_row['sc-status']).to eq('200')
      expect(second_row['cs-method']).to eq('GET')
      expect(second_row['cs-uri-stem']).to eq('/style.css')
    end
  end
end
