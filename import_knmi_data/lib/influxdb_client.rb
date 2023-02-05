require 'date'

require 'influxdb-client'

class InfluxDBClient
  def initialize(hostname:, org:, bucket:, token:, dry_run: false)
    @hostname = hostname
    @org = org
    @bucket = bucket
    @token = token
    @dry_run = dry_run
  end

  def client
    @client ||= InfluxDB2::Client.new("http://#{@hostname}:8086", @token,
                                      org: @org,
                                      bucket: @bucket,
                                      use_ssl: false,
                                      precision: InfluxDB2::WritePrecision::MILLISECOND)
  end

  def get_last_timestamp
    api = client.create_query_api.query(query: <<~QUERY
      from(bucket: "#{@bucket}")
        |> range(start: -30d, stop: now())
        |> filter(fn: (r) => r["_measurement"] == "weather")
        |> keep(columns: ["_time"])
        |> sort(columns: ["_time"], desc: false)
        |> last(column: "_time")
    QUERY
                                       )
  end

  def send_weather(row)
    data = {
      name: "weather",
      fields: {
        temperature: row.get_number("T"),
        average_wind_speed: row.get_number("FH"),
        average_wind_direction: row.get_number("DD"),
        cloud_cover: row.get_number("N"),
        sunshine_duration: row.get_number("SQ"),
        precipitation_duration: row.get_number("DR"),
        precipitation_amount: row.get_number("RH"),
        global_radiation: row.get_number("Q")

      },
      time: (row.timestamp.to_f * 1000).to_i
    }

    write_api.write(data: data) unless @dry_run
  end

  def write_api
    @write_api ||= client.create_write_api
  end
end

