module MyLogger
  def self.info(message)
    $stdout.puts message
    $stdout.flush
  end
end
