require 'sinatra/base'
require 'sinatra/reloader'

$LOAD_PATH << File.join(__dir__, "lib")

require "queries"

class App < Sinatra::Base
  configure :development do |config|
    register Sinatra::Reloader
    config.also_reload 'lib/*.rb'
  end

  get '/index' do
    Queries.gas_usage(DateTime.now - 10, DateTime.now, "1d").to_json
  end

  run!
end
