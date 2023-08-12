class QueryBuilder
  def initialize(bucket, field_name)
    @bucket = bucket
    @field_name = field_name
  end

  # laatste measurement meenemen: https://github.com/influxdata/influxdb/issues/6878#issuecomment-1269900903
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

  def include_previous
    @include_previous = true
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
