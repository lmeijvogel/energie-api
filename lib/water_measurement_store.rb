require 'date'
require 'redis'

class WaterMeasurementStore
  def initialize(redis_host:, measurements_redis_key: "water_meter_water_count", last_ticks_redis_key: "water_meter_last_ticks")
    @redis_host = redis_host
    @measurements_redis_key = measurements_redis_key
    @last_ticks_redis_key = last_ticks_redis_key
  end

  def get
    with_redis do |redis|
      redis.get(@measurements_redis_key).to_i
    end
  end

  def ticks
    with_redis do |redis|
      redis.lrange @last_ticks_redis_key, 0, 1
    end
  end
  private

  def with_redis
    redis = Redis.new(host: @redis_host)

    yield redis
  ensure
    redis.close
  end

end
