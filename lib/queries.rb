require "date"
require "influxdb-client"
require "my_logger"

class Queries
  def initialize(host, org, token, use_ssl)
    @host = host
    @org = org
    @token = token
    @use_ssl = use_ssl
  end

  def gas_usage(start, stop, window)
    query = QueryBuilder.new("readings", "gas")
      .range(start, stop, window)
      .aggregate_with("max()")
      .take_difference
      .build

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
          |> aggregateWindow(every: #{window}, fn: mean, createEmpty: false)
          |> yield(name: "#{output_name}")
      QUERY
    end.join("\n");

    with_client do |client|
      query_api = client.create_query_api

      results = query_api.query(query: query)

      %i[huiskamer tuinkamer zolder].each_with_object({}) do |sensor_name, result|
        sensor_result = results.values.find do |table|
          table.columns.find {|c| c.label == "result" }.default_value == sensor_name.to_s
        end

        result[sensor_name] = collect_rows(sensor_result)
      end
    end
  end

  def recent_power_usage(minutes = 10)
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

      {
        current: [[time, current - generation ]]
      }
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

  class QueryBuilder
    def initialize(bucket, field_name)
      @bucket = bucket
      @field_name = field_name
    end

    def build
      <<~QUERY
        #{imports}

        from(bucket: "#{@bucket}")
        |> range(start: #{@start.iso8601}, stop: #{@stop.iso8601})
        |> filter(fn: (r) => r._measurement == "#{@field_name}")
        |> window(every: #{@window}, createEmpty: false)
        |> #{@aggregate_with}
        |> duplicate(column: "_start", as: "_time")
        |> window(every: inf)
        #{"|> interpolate.linear(every: #{@window})" if @interpolate}
        #{"|> difference()" if @take_difference}
      QUERY
    end

    def range(start, stop, window)
      @start = start
      @stop = stop
      @window = window

      self
    end

    def aggregate_with(function)
      @aggregate_with = function

      self
    end

    def interpolate
      @interpolate = true

      self
    end

    def take_difference
      @take_difference = true

      self
    end


    private

    def imports
      <<~IMPORTS
        import "experimental"
        import "interpolate"
      IMPORTS
    end
  end

  def collect_rows(table)
    return [] if (table.nil?)

    table.records.map do |r|
      [r.time, r.value]
    end
  end

end
