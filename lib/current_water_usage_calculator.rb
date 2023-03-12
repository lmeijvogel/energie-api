require 'date'

module CurrentWaterUsageCalculator
  def self.calculate(timestamps)
    return 0 if timestamps.count < 2

    latest, first = timestamps

    prev_l_per_min = l_per_min(latest, first)
    current_l_per_min = l_per_min(DateTime.now, latest)

    [prev_l_per_min, current_l_per_min].min
  end

  def self.l_per_min(latest, prev)
    days_per_l = latest - prev
    min_per_l = days_per_l * 24 * 60

    ((1.0 / min_per_l) * 2).round * 0.5
  end
end
