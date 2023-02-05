class KnmiDataParser
  def initialize(data)
    @data = data.lines

    raise "Invalid data set" if @data.length == 0
  end

  def columns
    @_columns ||= begin
                    header_line = relevant_data[0]

                    positions = {}
                    start_index = 0

                    header_line.split(",").each_with_index do |entry, i|
                      name = entry.gsub(/#/, "").strip

                      positions[name] = i
                    end

                    positions
                  end
  end

  def rows
    # The data starts with an extensive comment explaining how to read it.
    relevant_data[1..-1].map.with_index {|line, i| InputRow.new(line, i + number_of_header_lines, columns) }
  end

  private
  def relevant_data
    @relevant_data ||= @data
      .drop_while { |row| !row.start_with?("# STN,YYYYMMDD") }
      .reject { |row| row.strip.empty? }
  end

  def number_of_header_lines
    @number_of_header_lines ||= @data.count - relevant_data.count
  end
end

class InputRow
  def initialize(line, line_number, columns)
    @line = line

    @line_number = line_number

    @columns = columns
  end

  def values(keys)
    keys.map { |key| get(key) }
  end

  def get_string(name, fallback_name = nil)
    index = field_position(name, fallback_name)

    @line.split(",")[index].strip
  end

  def get_number(name, fallback_name = nil)
    get_string(name, fallback_name).to_i(10)
  rescue StandardError => e
    STDERR.puts "Error parsing integer. Column: #{name}. line_number: #{@line_number}"

    raise
  end

  def timestamp
    date_string = get_string("yyyymmdd")
    hour = get_number("hh", "h") # Note: In the initial import, this field is named 'HH'

    year = Integer(date_string[0..3], 10)
    month = Integer(date_string[4..5], 10)
    day = Integer(date_string[6..7], 10)
    Time.new(year, month, day, hour)
  end

  def to_s
    %[#<#{self.class.name} #{timestamp}>];

  end

  private
  def field_position(title, fallback_title)
    sanitized_title = title.to_s.upcase

    if !@columns.include?(sanitized_title)
      sanitized_title = fallback_title.to_s.upcase
    end

    return @columns.fetch(sanitized_title)
  end
end
