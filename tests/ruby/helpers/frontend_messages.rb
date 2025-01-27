
class PostgresMessage
  # Base class for common functionality

  def encode_string(str)
    "#{str}\0"  # Encode a string with a null terminator
  end

  def encode_int16(value)
    [value].pack('n')  # Encode an Int16
  end

  def encode_int32(value)
    [value].pack('N')  # Encode an Int32
  end

  def message_prefix(type, length)
    "#{type}#{encode_int32(length)}"  # Message type and length prefix
  end
end

class SimpleQueryMessage < PostgresMessage
  attr_accessor :query

  def initialize(query = "")
    @query = query
  end

  def to_bytes
    query_bytes = encode_string(@query)
    length = 4 + query_bytes.size  # Length includes 4 bytes for length itself
    message_prefix('Q', length) + query_bytes
  end
end

class ParseMessage < PostgresMessage
  attr_accessor :statement_name, :query, :parameter_types

  def initialize(statement_name = "", query = "", parameter_types = [])
    @statement_name = statement_name
    @query = query
    @parameter_types = parameter_types
  end

  def to_bytes
    statement_name_bytes = encode_string(@statement_name)
    query_bytes = encode_string(@query)
    parameter_types_bytes = @parameter_types.pack('N*')
    
    length = 4 + statement_name_bytes.size + query_bytes.size + 2 + parameter_types_bytes.size
    message_prefix('P', length) + statement_name_bytes + query_bytes + encode_int16(@parameter_types.size) + parameter_types_bytes
  end
end

class BindMessage < PostgresMessage
  attr_accessor :portal_name, :statement_name, :parameter_format_codes, :parameters, :result_column_format_codes

  def initialize(portal_name = "", statement_name = "", parameter_format_codes = [], parameters = [], result_column_format_codes = [])
    @portal_name = portal_name
    @statement_name = statement_name
    @parameter_format_codes = parameter_format_codes
    @parameters = parameters
    @result_column_format_codes = result_column_format_codes
  end

  def to_bytes
    portal_name_bytes = encode_string(@portal_name)
    statement_name_bytes = encode_string(@statement_name)
    parameter_format_codes_bytes = @parameter_format_codes.pack('n*')
    
    parameters_bytes = @parameters.map do |param|
      if param.nil?
        encode_int32(-1)
      else
        encode_int32(param.bytesize) + param
      end
    end.join

    result_column_format_codes_bytes = @result_column_format_codes.pack('n*')

    length = 4 + portal_name_bytes.size + statement_name_bytes.size + 2 + parameter_format_codes_bytes.size + 2 + parameters_bytes.size + 2 + result_column_format_codes_bytes.size
    message_prefix('B', length) + portal_name_bytes + statement_name_bytes + encode_int16(@parameter_format_codes.size) + parameter_format_codes_bytes + encode_int16(@parameters.size) + parameters_bytes + encode_int16(@result_column_format_codes.size) + result_column_format_codes_bytes
  end
end

class DescribeMessage < PostgresMessage
  attr_accessor :type, :name

  def initialize(type = 'S', name = "")
    @type = type
    @name = name
  end

  def to_bytes
    name_bytes = encode_string(@name)
    length = 4 + 1 + name_bytes.size
    message_prefix('D', length) + @type + name_bytes
  end
end


class ExecuteMessage < PostgresMessage
  attr_accessor :portal_name, :max_rows

  def initialize(portal_name = "", max_rows = 0)
    @portal_name = portal_name
    @max_rows = max_rows
  end

  def to_bytes
    portal_name_bytes = encode_string(@portal_name)
    length = 4 + portal_name_bytes.size + 4
    message_prefix('E', length) + portal_name_bytes + encode_int32(@max_rows)
  end
end

class FlushMessage < PostgresMessage
  def to_bytes
    length = 4
    message_prefix('H', length)
  end
end

class SyncMessage < PostgresMessage
  def to_bytes
    length = 4
    message_prefix('S', length)
  end
end

class CloseMessage < PostgresMessage
  attr_accessor :type, :name

  def initialize(type = 'S', name = "")
    @type = type
    @name = name
  end

  def to_bytes
    name_bytes = encode_string(@name)
    length = 4 + 1 + name_bytes.size
    message_prefix('C', length) + @type + name_bytes
  end
end

