$LOAD_PATH << "./lib"

require "dotenv"

require "knmi_data_parser"
require "influxdb_client"
require 'ruby-progressbar'

Dotenv.load

DRY_RUN = false

influx = InfluxDBClient.new(
  hostname: ENV.fetch("INFLUXDB_HOST"),
  org: ENV.fetch("INFLUXDB_ORG"),
  bucket: ENV.fetch("INFLUXDB_BUCKET"),
  token: ENV.fetch("INFLUXDB_TOKEN"),
  dry_run: DRY_RUN)

parser = KnmiDataParser.new(ARGF.read)

progressbar = ProgressBar.create(total: parser.rows.count, format: "%t|%B| %c/%C")

parser.rows.each do |row|
  progressbar.increment
  influx.send_weather(row)
end
