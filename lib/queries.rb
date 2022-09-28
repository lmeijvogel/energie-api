require "date"
require "influxdb-client"
require "my_logger"

class Queries
  def initialize(host, org, token)
    @host = host
    @org = org
    @token = token
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
      .interpolate
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
          |> range(start: #{start}, stop: #{stop})
          |> filter(fn: (r) => r["entity_id"] == "#{table}")
          |> filter(fn: (r) => r["_field"] == "value")
          |> aggregateWindow(every: #{window}, fn: mean, createEmpty: false)
          |> yield(name: "#{output_name}")
      QUERY
    end.join("\n");

    client = InfluxDB2::Client.new(@host, @token, org: @org)
    query_api = client.create_query_api

    results = query_api.query(query: query)

    %i[huiskamer tuinkamer zolder].each_with_object({}) do |sensor_name, result|
      sensor_result = results.values.find do |table|
        table.columns.find {|c| c.label == "result" }.default_value == sensor_name.to_s
      end

      result[sensor_name] = collect_rows(sensor_result)
    end
  end

  def recent_power_usage
    query = <<~QUERY
      import "date"
      import "experimental"

      from(bucket:"readings")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "current")
      |> window(every: 30s)
      |> max()
      |> duplicate(column: "_start", as: "_time")
      |> window(every: inf)
    QUERY

    _perform_query(query)
  end

  def _perform_query(query)
    client = InfluxDB2::Client.new(@host, @token, org: @org)
    query_api = client.create_query_api

    result = query_api.query(query: query)

    result[0].records.map do |record|
      record.values.values_at("_time", "_value")
    end
  end

  class QueryBuilder
    def initialize(bucket, field_name)
      @bucket = bucket
      @field_name = field_name
    end

    def build
      optional_timezone = @window == "1h" ? timezone_import : nil

      <<~QUERY
        #{imports}
        #{optional_timezone}

        from(bucket: "#{@bucket}")
        |> range(start: #{@start}, stop: #{@stop})
        |> filter(fn: (r) => r._measurement == "#{@field_name}")
        |> window(every: #{@window}, createEmpty: true)
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

    def timezone_import
      <<~IMPORT
        import "timezone"

        option location = timezone.location(name: "Europe/Amsterdam")
      IMPORT
    end
  end

  def collect_rows(table)
    return [] if (table.nil?)

    table.records.map do |r|
      [r.time, r.value]
    end
  end

end
