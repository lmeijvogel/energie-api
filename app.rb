require 'dotenv'

require 'sinatra/base'
require 'sinatra/reloader'

$LOAD_PATH << File.join(__dir__, "lib")

require "queries"

Dotenv.load

class App < Sinatra::Base
  use Rack::CommonLogger

  set :logging, true

  configure :development do |config|
    register Sinatra::Reloader
    config.also_reload 'lib/*.rb'
  end

  configure :production do |config|
    set :bind, "0.0.0.0"
  end


  get '/api/:field/last_30_days' do
    today = DateTime.now()

    start_of_period = today - 30;

    case params[:field]
    when "gas"
      querier.gas_usage(start_of_period, today, "1h").to_json
    when "stroom"
      querier.stroom_usage(start_of_period, today, "1h").to_json
    when "water"
      querier.water_usage(start_of_period, today, "1h").to_json
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
    when "water"
      # Water is measured differently, so needs a different starting point
      querier.water_usage(start_of_period + 1, today, "1d").to_json
    end
  end

  get '/api/stroom/recent' do
    page = params[:page].to_i
    querier.recent_power_usage(page: page).to_json
  end

  get '/api/temperature/:location/:period/:year/?:month?/?:day?' do
    start, stop, window = get_query_range(params, "temperature")

    result = querier.temperature(params[:location], start, stop, window)

    result.to_json
  end

  get '/api/:field/:period/:year/?:month?/?:day?' do
    start, stop, window = get_query_range(params)

    result = case params[:field]
    when "gas"
      querier.gas_usage(start, stop, window)
    when "stroom"
      querier.stroom_usage(start, stop, window)
    when "water"
      querier.water_usage(start, stop, window)
    end

    result.to_json
  end


  def querier
    Queries.new(ENV.fetch("HOST"), ENV.fetch("ORG"), ENV.fetch("TOKEN"), ENV.fetch("USE_SSL") != "false")
  end

  def get_query_range(params, field_name = nil)
    case params[:period]
    when "day"
      day = Time.new(Integer(params[:year]), Integer(params[:month], 10), Integer(params[:day]))

      yesterday = day - 86400
      tomorrow = day + 86400

      end_of_yesterday = Time.new(yesterday.year, yesterday.month, yesterday.day, 23).to_datetime
      start_of_tomorrow = Time.new(tomorrow.year, tomorrow.month, tomorrow.day, 0).to_datetime

      [end_of_yesterday, start_of_tomorrow, "1h"]
    when "month"
      month = DateTime.new(Integer(params[:year]), Integer(params[:month]), 1)

      last_month = month << 1
      next_month = month >> 1

      start_of_this_month = DateTime.new(month.year, month.month, 1)
      end_of_previous_month = DateTime.new(last_month.year, last_month.month, -1)
      start_of_next_month = DateTime.new(next_month.year, next_month.month, 1)

      window = field_name == "temperature" ? "1h" : "1d";
      start_of_period = field_name == "temperature" ? start_of_this_month : end_of_previous_month;

      [start_of_period, start_of_next_month, window]
    when "year"
      year = Integer(params[:year])

      end_of_last_year = DateTime.new(year - 1, 12, 31)
      start_of_next_year = DateTime.new(year + 1, 1, 1)

      window = field_name == "temperature" ? "10d" : "1mo";
      [end_of_last_year, start_of_next_year,window]
    end
  end

  run!
end
