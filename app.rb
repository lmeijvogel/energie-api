require 'dotenv'

require 'sinatra/base'
require 'sinatra/reloader'

$LOAD_PATH << File.join(__dir__, "lib")

require "queries"

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

    MyLogger.info today
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
      querier.stroom_generation(start_of_period, today, "1d").to_json
    when "water"
      # Water is measured differently, so needs a different starting point
      querier.water_usage(start_of_period + 1, today, "1d").to_json
    end
  end

  get '/api/stroom/recent' do
    minutes = params[:minutes].to_i

    querier.recent_power_usage(minutes).to_json
  end

  get '/api/stroom/last' do
    page = params[:page].to_i
    querier.last_power_usage.to_json
  end

  get '/api/water/current' do
    querier.current_water_usage.to_json
  end

  get '/api/temperature/:location/:period/:year/?:month?/?:day?' do
    start, stop, window = get_query_range(params, 7, "temperature")

    result = querier.temperature(params[:location], start, stop, window)

    result.to_h.to_json
  end

  # Average over the week before
  get '/api/generation/aggregate/:fn/day/:year/:month/:day' do
    given_date = Date.new(Integer(params[:year]), Integer(params[:month]), Integer(params[:day]))

    querier.aggregated_generation(given_date - 1, params[:fn]).to_json
  end

  # Single year/month/day graphs
  get '/api/:field/:period/:year/?:month?/?:day?/?:window?' do
    start, stop, window = get_query_range(params, 7, params[:field])

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

  get '/api/:field/:period/:year/?:month?/?:day?/:count/?:window?' do
    start, stop, window = get_query_range(params, count.to_i || 1, params[:field])

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


  def querier
    Queries.new(ENV.fetch("HOST"), ENV.fetch("ORG"), ENV.fetch("TOKEN"), ENV.fetch("USE_SSL", true) != "false")
  end

  def get_query_range(params, count = 1, field_name = nil)
    case params[:period]
    when "day"
      day = Date.new(Integer(params[:year]), Integer(params[:month], 10), Integer(params[:day]))

      yesterday = day - 1
      day_after = day + count

      end_of_yesterday = Time.new(yesterday.year, yesterday.month, yesterday.day, 23).to_datetime
      start_of_day_after = Time.new(day_after.year, day_after.month, day_after.day, 0).to_datetime

      MyLogger.info "#{end_of_yesterday} - #{start_of_day_after}"
      window = "1h"

      [end_of_yesterday, start_of_day_after, window]
    when "month"
      month = Date.new(Integer(params[:year]), Integer(params[:month]), 1)

      day_before_month = month.prev_day
      month_after = month >> count

      start_of_this_month = Time.new(month.year, month.month, month.day)
      end_of_previous_month = Time.new(day_before_month.year, day_before_month.month, day_before_month.day)
      start_of_month_after = Time.new(month_after.year, month_after.month, month_after.day)

      window = field_name == "temperature" ? "1h" : "1d";
      start_of_period = field_name == "temperature" ? start_of_this_month : end_of_previous_month;

      [start_of_period, start_of_month_after, window]
    when "year"
      year = Integer(params[:year])

      end_of_last_year = Date.new(year - 1, 12, 31)
      start_of_year_after = Date.new(year + count, 1, 1)

      window = field_name == "temperature" ? "10d" : "1mo";
      [end_of_last_year, start_of_year_after, window]
    end
  end

  run!
end
