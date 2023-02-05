require 'spec_helper'

require 'knmi_data_parser'

describe KnmiDataParser do
  it "should handle an empty input" do
    expect {
      parser = KnmiDataParser.new("")
    }.to raise_error(StandardError)
  end

  it "should handle a file without data" do
    parser = KnmiDataParser.new(fixture("empty"))

    expect(parser.rows).to be_empty
  end

  it "should return rows" do
    parser = KnmiDataParser.new(fixture("with_data"))

    expect(parser.rows.length).to eql(11)
  end

  it "should read data" do
    rows = KnmiDataParser.new(fixture("with_data")).rows

    expect(rows[0].get_number("stn")).to eql(344)
    expect(rows[1].get_string("hh")).to eql("2")

    # "DD" is also a substring of the date (YYYYMMDD
    expect(rows[0].get_number("dd")).to eql(240)

    expect(rows[10].get_number("n")).to eql(0)
  end

  it "should raise a KeyError if the requested field does not exist" do
    rows = KnmiDataParser.new(fixture("with_data")).rows

    expect {
      rows[0].get_string("boo")
    }.to raise_error(KeyError)
  end

  it "should use a fallback field name if the requested field does not exist" do
    rows = KnmiDataParser.new(fixture("with_data")).rows

    expect(rows[0].get_string("boo", "T")).to eql("52")
  end

  it "should generate a Time for each row" do
    rows = KnmiDataParser.new(fixture("with_data")).rows

    actual = rows[0].timestamp
    expect(actual).to eql(Time.new(2011, 1, 1, 1))

    # "08" is interpreted as octal, by default.
    with_08 = rows[9].timestamp
    expect(with_08).to eql(Time.new(2011, 1, 8, 10))
  end

  def fixture(name)
    path = File.join(__dir__, "fixtures", name)

    File.read(path)
  end
end
