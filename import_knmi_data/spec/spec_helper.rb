$LOAD_PATH << File.join(__dir__,  "../lib")

RSpec.configure do |config|
  config.filter_run_when_matching focus: true
end
