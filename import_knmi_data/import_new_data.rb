$LOAD_PATH << "./lib"

require 'uri'
require 'net/http'

require "date"
require "dotenv"

require "knmi_data_parser"
require "influxdb_client"
require 'ruby-progressbar'

Dotenv.load

DRY_RUN = false

# CACHE_FILENAME = "/tmp/knmi_cache.txt"
CACHE_FILENAME = nil

def main
  influx = InfluxDBClient.new(
    hostname: ENV.fetch("INFLUXDB_HOST"),
    org: ENV.fetch("INFLUXDB_ORG"),
    bucket: ENV.fetch("INFLUXDB_BUCKET"),
    token: ENV.fetch("INFLUXDB_TOKEN"),
    dry_run: DRY_RUN)

  response = influx.get_last_timestamp

  last_input_date = Date.iso8601(response[0].records[0].row[2])
  today = Date.today

  # Adding an extra field at the end because fixing parsing is difficult
  fields = %w[T FH DD N SQ DR RH Q U].join(":")
  url = %{https://www.daggegevens.knmi.nl/klimatologie/uurgegevens?start=#{format_query_date(last_input_date)}&end=#{format_query_date(today)}&vars=#{fields}&stns=344}

  data = get_data(url)

  parser = KnmiDataParser.new(data)

  progressbar = ProgressBar.create(total: parser.rows.count, format: "%t|%B| %c/%C")

  parser.rows.each do |row|
    progressbar.increment
    influx.send_weather(row)
  end
end

def format_query_date(date)
  date.strftime("%Y%m%d0000")
end

def get_data(url)
  if CACHE_FILENAME && File.exist?(CACHE_FILENAME)
    puts "NOTE: Reading from cache file!"
    return File.read(CACHE_FILENAME)
  end

  uri = URI(url)
  res = Net::HTTP.get_response(uri)

  if res.is_a?(Net::HTTPSuccess)
    body = res.body

    if CACHE_FILENAME
      File.open(CACHE_FILENAME, "w") do |file|
        file.write(body)
      end
    end

    return body
  else
    raise StandardError("Could not get data")
  end
end

main
