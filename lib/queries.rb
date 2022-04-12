require "date"
require "influxdb-client"

module Queries
  def self.gas_usage(start, stop, window)
    query = <<~QUERY
      import "experimental"

      from(bucket:"readings")
      |> range(start: #{start}, stop: #{stop})
      |> filter(fn: (r) => r._measurement == "gas")
      |> window(every: #{window})
      |> max()
      |> duplicate(column: "_start", as: "_time")
      |> window(every: inf)
      |> difference()
      |> map(fn: (r) => ({ r with _value: if r._value > 0 then r._value else 0.0 }))
    QUERY

    query_api = client.create_query_api

    result = query_api.query(query: query)

    data_array = result[0].records.map do |record|
      record.values.values_at("_time", "_value")
    end
  end
end
