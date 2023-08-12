require "date"
require "influxdb-client"
require "my_logger"
require "query_builder"

class Queries
  def initialize(host, org, token, use_ssl)
    @host = host
    @org = org
    @token = token
    @use_ssl = use_ssl
  end

  def gas_usage(start, stop, window)
    query = <<~QUERY
      import "date"
      import "experimental"
      import "interpolate"

      prefix_query = from(bucket: "readings")
        |> range(start: date.sub(d: 24h, from: #{start.iso8601}), stop: #{start.iso8601})
        |> filter(fn: (r) => r._measurement == "gas")
        |> last()

      data_range = from(bucket: "readings")
        |> range(start: #{start.iso8601}, stop: #{stop.iso8601})
        |> filter(fn: (r) => r._measurement == "gas")
        |> window(every: #{window}, createEmpty: false)
        |> max()
        |> duplicate(column: "_start", as: "_time")
        |> window(every: inf)

      union(tables: [prefix_query, data_range])
        |> group(columns:["_field"])
        |> difference()
    QUERY

    # Do not interpolate, since missing measurements regularly occur: Only changes in gas usage
    # are logged.
    _perform_query(query)
  end

  def stroom_usage(start, stop, window)
    query = QueryBuilder.new("readings", "stroom")
      .range(start, stop, window)
      .aggregate_with("max()")
      .take_difference
      .build

    _perform_query(query)
  end

  def stroom_generation(start, stop, window)
    query = QueryBuilder.new("readings", "opwekking")
      .range(start, stop, window)
      .aggregate_with("sum()")
      .build

    _perform_query(query)
  end

  def stroom_back_delivery(start, stop, window)
    query = QueryBuilder.new("readings", "levering")
      .range(start, stop, window)
      .aggregate_with("max()")
      .take_difference
      .build

    _perform_query(query)
  end

  def water_usage(start, stop, window)
    query = QueryBuilder.new("readings", "water")
      .range(start, stop, window)
      .aggregate_with("count()")
      .build

    _perform_query(query)
  end

  def temperature(location, start, stop, window)
    query = [
      { table: "motionsensor_air_temperature", output_name: "huiskamer" },
      { table: "motionsensor_zolder_air_temperature", output_name: "zolder" },
      { table: "motionsensor_tuinkamer_air_temperature", output_name: "tuinkamer" }
    ].map do |query_params|
      table = query_params.fetch(:table)
      output_name = query_params.fetch(:output_name)

      <<~QUERY
        from(bucket: "sensors")
          |> range(start: #{start.iso8601}, stop: #{stop.iso8601})
          |> filter(fn: (r) => r["entity_id"] == "#{table}")
          |> filter(fn: (r) => r["_field"] == "value")
          |> aggregateWindow(every: #{window}, fn: max, createEmpty: false)
          |> yield(name: "#{output_name}")
      QUERY
    end.join("\n");

    query += <<~QUERY
        from(bucket: "weather")
          |> range(start: #{start.iso8601}, stop: #{stop.iso8601})
          |> filter(fn: (r) => r["_measurement"] == "weather")
          |> filter(fn: (r) => r["_field"] == "temperature")
          // Values from KNMI are in tenths of degrees
          |> map(fn: (r) => ({ r with _value: float(v: r._value) / 10.0 }))
          |> aggregateWindow(every: #{window}, fn: max, createEmpty: false)
          |> yield(name: "buiten")
    QUERY

    with_client do |client|
      query_api = client.create_query_api

      results = query_api.query(query: query)

      %i[huiskamer tuinkamer zolder buiten].each_with_object({}) do |sensor_name, result|
        sensor_result = results.values.find do |table|
          table.columns.find {|c| c.label == "result" }.default_value == sensor_name.to_s
        end

        result[sensor_name] = collect_rows(sensor_result)
      end
    end
  end

  def recent_power_usage(minutes = 60)
    query = <<~QUERY
      from(bucket: "readings_last_hour")
        |> range(start: -#{minutes}m)
        |> filter(fn: (r) => r["_measurement"] == "current")
        |> filter(fn: (r) => r["_field"] == "current" or r["_field"] == "generation")
        |> aggregateWindow(every: 6s, fn: mean, createEmpty: false)
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> map(fn: (r) => ({ r with _value: r.current - r.generation }))
        |> yield(name: "current")
    QUERY

    with_client do |client|
      query_api = client.create_query_api

      results = query_api.query(query: query)

      %i[current generation].each_with_object({}) do |sensor_name, result|
        sensor_result = results.values.find do |table|
          table.columns.find {|c| c.label == "result" }.default_value == sensor_name.to_s
        end

        result[sensor_name] = collect_rows(sensor_result)
      end
    end
  end

  def recent_water_usage(minutes = 60)
    # Since water usage is less frequent than power usage,
    # we want to show the last hour where water was used,
    # otherwise the graph will be empty most of the day.
    #
    # This does require some extra work: Getting the last
    # stored water row. Below I first get that and with some
    # conversion get the right range.
    #
    # I got the idea here: https://docs.influxdata.com/influxdb/v2.2/query-data/flux/scalar-values/
    query = <<~QUERY
      import "date"

      // Define a helper function to extract a row as a record
      getRow = (tables=<-, field, idx=0) => {
          extract = tables
              |> findRecord(fn: (key) => true, idx: idx)

          return extract
      }


      getLast = () => {
        _last = from(bucket: "readings")
              |> range(start: -1d)
              |> filter(fn: (r) => r["_measurement"] == "water")
              |> filter(fn: (r) => r["_field"] == "water")
              |> last()
              |> getRow(field: "_time")

        return _last
      }

      // A lot of work just to get the last row with values instead
      // of as a stream.
      last = getLast()

      from(bucket: "readings")
        |> range(start: date.sub(from: last["_time"], d: #{minutes}m), stop: last["_time"])
        |> filter(fn: (r) => r["_measurement"] == "water")
        |> filter(fn: (r) => r["_field"] == "water")
        |> aggregateWindow(every: 1m, fn: count, createEmpty: true)
        |> yield(name: "count")
    QUERY

    _perform_query(query)
  end

  def last_power_usage
    query = <<~QUERY
      from(bucket: "readings_last_hour")
        |> range(start: -2s)
        |> filter(fn: (r) => r["_measurement"] == "current" and (r["_field"] == "current" or r["_field"] == "generation"))
        |> last()
    QUERY

    with_client do |client|
      query_api = client.create_query_api

      results = query_api.query(query: query)

      values = results.map {|i, r| r.records[0].values }

      current, time = values.find {|v| v["_field"] == "current" }.values_at("_value", "_time")

      generation_entry = values.find {|v| v["_field"] == "generation" }
      generation = generation_entry ? generation_entry["_value"] : 0

      current - generation
    end
  end

  def aggregated_generation(end_date, fn)
    start_date = end_date - 7

    aggregate_fn = case fn
                   when "mean" then 'mean(column: "_value")'
                   when "max" then 'max(column: "_value")'
                   end
    query = <<~QUERY
      import "date"
      import "math"

      from(bucket: "readings")
        |> range(start: #{start_date.iso8601}, stop: #{end_date.iso8601})
        |> filter(fn: (r) => r["_measurement"] == "opwekking")
        |> filter(fn: (r) => r["_field"] == "opwekking")
        |> filter(fn: (r) => math.remainder(x: float(v: date.minute(t: r["_time"])), y: 15.0) == 0.0)
        |> map(fn: (r) => ({
            r with hour: date.hour(t: r._time), minute: date.minute(t: r._time)
        }))
        |> group(columns: ["hour", "minute"], mode:"by")
        |> #{aggregate_fn}
    QUERY

    result = []

    with_client do |client|
      query_api = client.create_query_api
      result = query_api.query(query: query)

      result.map {|i, r| r.records[0].values.values_at("hour", "minute", "_value") }
    end
  end

  def _perform_query(query)
    with_client do |client|
      query_api = client.create_query_api

      result = query_api.query(query: query)

      return [] if !result || !result[0]
      result[0].records.map do |record|
        record.values.values_at("_time", "_value")
      end
    end
  end

  private
  def with_client
    client = InfluxDB2::Client.new(@host, @token, org: @org, use_ssl: @use_ssl)

    yield client
  end
end
