require 'dotenv'

require 'fileutils'

require 'sinatra/base'
require 'sinatra/reloader'

$LOAD_PATH << File.join(__dir__, "lib")

CACHE_DIR = File.join(__dir__, "tmp", "cache")

if !File.directory?(CACHE_DIR)
  FileUtils.mkdir_p(CACHE_DIR)
end

require "queries"
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


  get '/api/:field/last_30_days' do
    today = Date.today

    start_of_period = (today - 30).to_time
    now = Time.now

    case params[:field]
    when "gas"
      querier.gas_usage(start_of_period, now, "1h").to_json
    when "stroom"
      querier.stroom_usage(start_of_period, now, "1h").to_json
    when "generation"
      querier.stroom_generation(start_of_period, now, "1h").to_json
    when "back_delivery"
      querier.stroom_back_delivery(start_of_period, now, "1h").to_json
    when "water"
      querier.water_usage(start_of_period, now, "1h").to_json
    end
  end

  get '/api/:field/last_year' do
    today = DateTime.now()

    start_of_period = (DateTime.new(today.year - 1, today.month, 1)) - 1;

    case params[:field]
    when "gas"
      querier.gas_usage(start_of_period, today, "1d").to_json
    when "stroom"
      querier.stroom_usage(start_of_period, today, "1d").to_json
    when "back_delivery"
      querier.stroom_back_delivery(start_of_period, today, "1d").to_json
    when "generation"
      # Generation is measured differently, so needs a different starting point
      querier.stroom_generation(start_of_period + 1, today, "1d").to_json
    when "water"
      # Water is measured differently, so needs a different starting point
      querier.water_usage(start_of_period + 1, today, "1d").to_json
    end
  end

  get '/api/stroom/recent' do
    minutes = params[:minutes].to_i

    querier.recent_power_usage(minutes).to_json
  end

  get '/api/water/recent' do
    minutes = params[:minutes].to_i

    querier.recent_water_usage(60).to_json
  end

  get '/api/usage/last' do
    water_measurement_store = WaterMeasurementStore.new(redis_host: ENV.fetch("REDIS_HOST"))

    last_water_ticks_redis = water_measurement_store.ticks
    last_water_ticks = last_water_ticks_redis.map { |str| DateTime.parse(str) }

    water_current = CurrentWaterUsageCalculator.calculate(last_water_ticks)

    {
          current: querier.last_power_usage.to_json,
          water: water_current
    }.to_json
  end

  get '/api/temperature/:location/:period/:year/?:month?/?:day?' do
    start, stop, window = get_query_range(params, "temperature")

    result = querier.temperature(params[:location], start, stop, window)

    result.to_h.to_json
  end

  # Average over the week before
  get '/api/generation/aggregate/:fn/day/:year/:month/:day' do
    with_cache("generation_aggregate_#{params[:fn]}", params) do
      given_date = Date.new(Integer(params[:year]), Integer(params[:month]), Integer(params[:day]))

      querier.aggregated_generation(given_date - 1, params[:fn]).to_json
    end
  end

  get '/api/:field/:period/:year/?:month?/?:day?/?:window?' do
    with_cache("period", params) do
      start, stop, window = get_query_range(params, params[:field])

      window = params[:window] if params[:window]

      result = case params[:field]
      when "gas"
        querier.gas_usage(start, stop, window)
      when "stroom"
        querier.stroom_usage(start, stop, window)
      when "back_delivery"
        querier.stroom_back_delivery(start, stop, window)
      when "generation"
        querier.stroom_generation(start, stop, window)
      when "water"
        querier.water_usage(start, stop, window)
      end

      result.to_json
    end
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
    Queries.new(ENV.fetch("INFLUXDB_HOST"), ENV.fetch("INFLUXDB_ORG"), ENV.fetch("INFLUXDB_TOKEN"), ENV.fetch("INFLUXDB_USE_SSL", true) != "false")
  end

  def get_query_range(params, field_name = nil)
    case params[:period]
    when "day"
      day = Date.new(Integer(params[:year]), Integer(params[:month], 10), Integer(params[:day]))

      yesterday = day - 1
      tomorrow = day + 1

      start = if field_name == "generation"
                day.to_datetime
              else
                Time.new(yesterday.year, yesterday.month, yesterday.day, 23).to_datetime
              end

      start_of_tomorrow = Time.new(tomorrow.year, tomorrow.month, tomorrow.day, 0).to_datetime

      window = "1h"

      [start, start_of_tomorrow, window]
    when "month"
      month = Date.new(Integer(params[:year]), Integer(params[:month]), 1)

      day_before_month = month.prev_day
      next_month = month.next_month

      start_of_this_month = Time.new(month.year, month.month, month.day)
      end_of_previous_month = Time.new(day_before_month.year, day_before_month.month, day_before_month.day)
      start_of_next_month = Time.new(next_month.year, next_month.month, next_month.day)

      window = field_name == "temperature" ? "1h" : "1d";
      start_of_period = field_name == "temperature" ? start_of_this_month : end_of_previous_month;

      [start_of_period, start_of_next_month, window]
    when "year"
      year = Integer(params[:year])

      end_of_last_year = Date.new(year - 1, 12, 31)
      start_of_next_year = Date.new(year + 1, 1, 1)

      window = field_name == "temperature" ? "10d" : "1mo";
      [end_of_last_year, start_of_next_year, window]
    end
  end

  run!
end
