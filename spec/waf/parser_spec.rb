require 'spec_helper'
require_relative '../../lib/log_tool/waf/parser'
require_relative '../../lib/log_tool/common/logger'
require 'fileutils'
require 'csv'
require 'json'

RSpec.describe LogTool::Waf::Parser do
  let(:logger) { LogTool::Common::AppLogger.build }
  let(:sample_log_path) { File.join(File.dirname(__FILE__), '../fixtures/sample_waf_log.txt') }
  let(:output_path) { File.join(File.dirname(__FILE__), '../tmp/waf_output.csv') }

  before do
    # テスト出力ディレクトリを作成
    FileUtils.mkdir_p(File.dirname(output_path))
  end

  after do
    # テスト後にファイルをクリーンアップ
    FileUtils.rm_f(output_path)
  end

  describe '#to_csv' do
    it 'WAFログをCSVに変換する' do
      # サンプルWAFログファイルを読み込む
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
      expect(first_row['timestamp']).to eq('1746580815728')
      expect(first_row['formatVersion']).to eq('1')
      expect(first_row['action']).to eq('ALLOW')
      expect(first_row['clientIp']).to eq('192.168.1.100')
      expect(first_row['uri']).to eq('/index.html')
      expect(first_row['httpMethod']).to eq('GET')

      # 2行目の内容を検証
      second_row = csv_content[1]
      expect(second_row['timestamp']).to eq('1746580815820')
      expect(second_row['clientIp']).to eq('192.168.1.101')
      expect(second_row['uri']).to eq('/products')
      expect(second_row['args']).to eq('category=electronics')

      # ヘッダーとラベルがJSON文字列として正しく出力されているか検証
      first_row_headers = JSON.parse(first_row['headers'])
      expect(first_row_headers).to be_an(Array)
      expect(first_row_headers.first['name']).to eq('host')
      expect(first_row_headers.first['value']).to eq('example.com')

      first_row_labels = JSON.parse(first_row['labels'])
      expect(first_row_labels).to be_an(Array)
      expect(first_row_labels.first['name']).to eq('awswaf:managed:token:absent')
    end

    it 'JSONでない行はスキップする' do
      # 不正な行を含むコンテンツ
      invalid_content = "This is not a JSON line\n" + File.read(sample_log_path)
      parser = described_class.new(invalid_content, logger)

      # CSVに変換
      count = parser.to_csv(output_path)

      # 変換結果を検証（不正な行はスキップされるはず）
      expect(count).to eq(2)
      expect(File.exist?(output_path)).to be true
    end
  end
end
