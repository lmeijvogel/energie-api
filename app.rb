require 'dotenv'

require 'fileutils'

require 'pg'

require 'sinatra/base'
require 'sinatra/reloader'

$LOAD_PATH << File.join(__dir__, "lib")

CACHE_DIR = File.join(__dir__, "tmp", "cache")

if !File.directory?(CACHE_DIR)
  FileUtils.mkdir_p(CACHE_DIR)
end

require "water_measurement_store"
require "current_water_usage_calculator"

Dotenv.load

class App < Sinatra::Base
  set :logging, true

  configure :development do |config|
    register Sinatra::Reloader
    config.also_reload 'lib/*.rb'
  end

  configure :production do |config|
    set :bind, "0.0.0.0"
  end


  get '/api/:field/hourly/:days' do
    today = Date.today

    start_of_period = (today - Integer(params[:days])).to_time

    perform_heatmap_query(params[:field], start_of_period, "1 hour")
  end

  get '/api/:field/last_year' do
    today = Date.today

    start_of_period = (DateTime.new(today.year - 1, today.month, 1)) - 1;

    perform_heatmap_query(params[:field], start_of_period, "1 day")
  end

  def perform_heatmap_query(field_in_params, start_of_period, bucket_size)
    now = Time.now

    table_name, field = get_table_name_and_field(field_in_params)

    result = with_pg do |connection|
      query = case table_name
      when "gas", "power"
        query_for_cumulative_source(table_name, field)
      when "generation", "water"
        query_for_usage_source(table_name, field)
      end

      connection.exec(query, [start_of_period, now, bucket_size])
    end

    postprocessing = case field
                     when "cumulative_total_dm3", "cumulative_from_network_wh", "cumulative_to_network_wh" then proc {|usage| usage.to_f / 1000 }
                     when "usage_dl" then proc {|usage| usage.to_f / 10 }
                     else proc { |u| u.to_f }
                     end

    result.select {|entry| entry["usage"] != nil}.map {|entry| [entry["bucket"], postprocessing.(entry["usage"])] }.to_json
  end

  get '/api/stroom/recent' do
    with_redis do |redis|
      data = redis.lrange("recent_current_measurements", 0, -1)

      data.map {|row| JSON.parse(row) }.to_json
    end
  end

  get '/api/water/recent' do
    minutes = params[:minutes].to_i

    water_measurement_store = WaterMeasurementStore.new(redis_host: ENV.fetch("REDIS_HOST"))
    last_tick = water_measurement_store.ticks[0] || Time.now

    with_pg do |connection|
      query = <<~QUERY
        SELECT
          time_bucket('1 minute'::interval, created, 'Europe/Amsterdam') AS bucket,
          SUM(usage_dl) AS usage_dl
          FROM water WHERE created >
            (SELECT created FROM water ORDER BY created DESC LIMIT 1) - $1::interval
          GROUP BY bucket ORDER BY bucket;
      QUERY

      interval = "#{minutes} minutes"
      result = connection.exec(query, [interval])
      result.select {|entry| entry["usage_dl"] != nil}.map do |entry|
        [
          entry["bucket"], entry["usage_dl"].to_f / 10
        ]
      end.to_json
    end
  end

  get '/api/usage/last' do
    water_measurement_store = WaterMeasurementStore.new(redis_host: ENV.fetch("REDIS_HOST"))

    last_water_ticks_redis = water_measurement_store.ticks
    last_water_ticks = last_water_ticks_redis.map { |str| DateTime.parse(str) }

    water_current = CurrentWaterUsageCalculator.calculate(last_water_ticks)

    last_power_usage_w = with_redis do |redis|
      redis.get("last_current_measurement").to_f / 1000
    end

    {
          current: last_power_usage_w,
          water: water_current
    }.to_json
  end

  get '/api/temperature/:location/:period/:year/?:month?/?:day?' do
    start, stop, bucket_size = get_query_range(params, "temperature")

    with_pg do |connection|
      query = <<~QUERY
                  SELECT time_bucket($3::interval, created, 'Europe/Amsterdam') AS bucket,
                    MAX(huiskamer) AS huiskamer,
                    MAX(tuinkamer) AS tuinkamer,
                    MAX(zolder) AS zolder
                  FROM temperatures
                  WHERE $1::timestamp < created AND created < $2::timestamp
                  GROUP BY bucket ORDER BY bucket
      QUERY
      result = connection.exec(query, [start, stop, bucket_size])

      result.map do |entry|
        {
          timestamp: entry["bucket"],
          huiskamer: entry["huiskamer"].to_f / 10,
          tuinkamer: entry["tuinkamer"].to_f / 10,
          zolder: entry["zolder"].to_f / 10
        }
      end.to_json
    end
  end

  # Average over the week before
  get '/api/generation/aggregate/:fn/day/:year/:month/:day' do
    with_cache("generation_aggregate_#{params[:fn]}", params) do
      given_date = Date.new(Integer(params[:year]), Integer(params[:month]), Integer(params[:day]))

      start = given_date - 7

      table_name, field = get_table_name_and_field("generation")

      fn = case params[:fn]
                         when "max" then "MAX"
                         when "mean" then "AVG"
                         end

      query = <<~QUERY
                  WITH all_measurements AS (
                  SELECT time_bucket($3::interval, created, 'Europe/Amsterdam') AS bucket,
                    #{fn}(#{field}) AS usage
                  FROM #{table_name}
                  WHERE $1::timestamp < created AND created < $2::timestamp
                  GROUP BY bucket ORDER BY bucket)

                  SELECT EXTRACT(HOUR from bucket) as hour, EXTRACT(MINUTE from bucket) as minute, #{fn}(usage) as usage
                  FROM all_measurements
                  GROUP BY hour, minute
                  ORDER BY hour, minute
      QUERY


      with_pg do |connection|
        result = connection.exec(query, [start, given_date, "15 minutes"])

        result.select {|entry| entry["usage"] != nil}.map {|entry| [entry["hour"].to_i, entry["minute"].to_i, entry["usage"].to_i] }.to_json
      end
    end
  end

  get '/api/:field/:period/:year/?:month?/?:day?/?:window?' do
    with_cache("period", params) do
      start, stop, bucket_size = get_query_range(params, params[:field])

      bucket_size = params[:window] if params[:window]

      table_name, field = get_table_name_and_field(params[:field])

      with_pg do |connection|
        query = if table_name == "gas" || table_name == "power"
                  query_for_cumulative_source(table_name, field)
                else
                  query_for_usage_source(table_name, field)
                end

        result = connection.exec(query, [start, stop, bucket_size])

        postprocessing = case field
                         when "cumulative_total_dm3", "cumulative_from_network_wh", "cumulative_to_network_wh" then proc {|usage| usage.to_f / 1000 }
                         when "usage_dl" then proc {|usage| usage.to_f / 10 }
                         else proc { |u| u.to_f }
                         end

        result.select {|entry| entry["usage"] != nil}.map {|entry| [entry["bucket"], postprocessing.(entry["usage"])] }.to_json
      end
    end
  end

  def with_pg
    PG::Connection.open(host: ENV.fetch("POSTGRES_HOST"), dbname: ENV.fetch("POSTGRES_DATABASE"), user: ENV.fetch("POSTGRES_USER"), password: ENV.fetch("POSTGRES_PASSWORD")) do |connection|
      yield connection
    end
  end

  def with_redis
    redis = Redis.new(host: ENV.fetch("REDIS_HOST"))

    yield redis
  ensure
    redis.close
  end

  def query_for_cumulative_source(table_name, field)
    <<~QUERY
                  WITH bucketed as (
                    SELECT
                      time_bucket($3::interval, created, 'Europe/Amsterdam') as bucket,
                      counter_agg(created, #{field})
                    FROM #{table_name}
                    WHERE $1::timestamp < created AND created < $2::timestamp

                    GROUP BY bucket
                    ORDER BY bucket)

                  SELECT
                    bucket,
                    interpolated_delta(
                      counter_agg,
                      bucket,
                      $3::interval,
                      lag(counter_agg) OVER ordered_meter,
                      lead(counter_agg) OVER ordered_meter) as usage
                  FROM bucketed
                  WINDOW ordered_meter AS (ORDER BY bucket)
                  ORDER BY bucket;

    QUERY
  end

  def query_for_usage_source(table_name, field)
    <<~QUERY
                  SELECT time_bucket($3::interval, created, 'Europe/Amsterdam') AS bucket,
                    SUM(#{field}) AS usage
                  FROM #{table_name}
                  WHERE $1::timestamp < created AND created < $2::timestamp
                  GROUP BY bucket ORDER BY bucket
    QUERY
  end

  def with_cache(cache_category, params)
    return yield if current_period?(params)

    cache_key = params.map do |key, value|
      "#{sanitize_for_filename(key)}_#{sanitize_for_filename(value)}"
    end.join("__")

    filename = File.join(CACHE_DIR, "#{cache_category}___#{cache_key}.json")

    if File.exist?(filename)
      return File.read(filename)
    end

    value = yield

    File.open(filename, "w") do |file|
      file.write(value)
    end

    return value
  end

  def current_period?(params)
    year = params[:year].to_i
    month = params[:month].to_i
    day = params[:day].to_i

    today = Date.today

    case params[:period]
    when "day"
      return Date.new(year, month, day) == today
    when "month"
      return Date.new(year, month, 1) == Date.new(today.year, today.month, 1)
    when "year"
      return Date.new(year, 1, 1) == Date.new(today.year, 1, 1)
    end

    false
  end

  def sanitize_for_filename(input)
    input.to_s.gsub(/[^A-Za-z0-9\-.]/, "-")
  end

  def querier
    Queries.new(ENV.fetch("INFLUXDB_HOST"), ENV.fetch("INFLUXDB_ORG"), ENV.fetch("INFLUXDB_BUCKET"), ENV.fetch("INFLUXDB_TOKEN"), ENV.fetch("INFLUXDB_USE_SSL", true) != "false")
  end

  def get_query_range(params, field_name = nil)
    one_second = 1.0 / 24 / 60 / 60

    case params[:period]
    when "day"
      start_of_today = DateTime.new(Integer(params[:year]), Integer(params[:month], 10), Integer(params[:day], 10))
      start_of_tomorrow = start_of_today + 1

      end_of_today = start_of_tomorrow - one_second

      [start_of_today, end_of_today, "1 hour"]
    when "month"
      start_of_this_month = DateTime.new(Integer(params[:year]), Integer(params[:month]), 1)
      start_of_next_month = start_of_this_month >> 1

      end_of_this_month = start_of_next_month - one_second

      bucket_size = field_name == "temperature" ? "1 hour" : "1 day";

      [start_of_this_month, end_of_this_month, bucket_size]
    when "year"
      year = Integer(params[:year])

      start_of_year = DateTime.new(year, 1, 1)
      end_of_year = DateTime.new(year, 12, 31, 23, 59, 59)

      bucket_size = field_name == "temperature" ? "10 days" : "1 month";
      [start_of_year, end_of_year, bucket_size]
    end
  end


  def get_table_name_and_field(field_in_params)
    case field_in_params
    when "gas" then ["gas", "cumulative_total_dm3"]
    when "water" then ["water", "usage_dl"]
    when "stroom" then ["power", "cumulative_from_network_wh"]
    when "back_delivery" then ["power", "cumulative_to_network_wh"]
    when "generation" then ["generation", "generation_wh"]
    end
  end

  run!
end
