require 'socket'
require 'digest/md5'
require_relative 'frontend_messages'

BACKEND_MESSAGE_CODES = {
  'Z' => "ReadyForQuery",
  'C' => "CommandComplete",
  'T' => "RowDescription",
  'D' => "DataRow",
  '1' => "ParseComplete",
  '2' => "BindComplete",
  'E' => "ErrorResponse",
  's' => "PortalSuspended",
}

class PostgresSocket
  def initialize(host, port)
    @port = port
    @host = host
    @socket = TCPSocket.new @host, @port
    @parameters = {}
    @verbose = false
  end

  def send_message(message)
    @socket.write(message.to_bytes)
  end
  
  def send_md5_password_message(username, password, salt)
    m = Digest::MD5.hexdigest(password + username)
    m = Digest::MD5.hexdigest(m + salt.map(&:chr).join(""))
    m = 'md5' + m
    bytes = (m.split("").map(&:ord) + [0]).flatten
    message_size = bytes.count + 4

    message = []

    message << 'p'.ord
    message << [message_size].pack('l>').unpack('CCCC') # 4
    message << bytes
    message.flatten!


    @socket.write(message.pack('C*'))
  end

  def send_startup_message(username, database, password)
    message = []

    message << [196608].pack('l>').unpack('CCCC') # 4
    message << "user".split('').map(&:ord) # 4, 8
    message << 0 # 1, 9
    message << username.split('').map(&:ord) # 2, 11
    message << 0 # 1, 12
    message << "database".split('').map(&:ord) # 8, 20
    message << 0 # 1, 21
    message << database.split('').map(&:ord) # 2, 23
    message << 0 # 1, 24
    message << 0 # 1, 25
    message.flatten!

    total_message_size = message.size + 4

    message_len = [total_message_size].pack('l>').unpack('CCCC')

    @socket.write([message_len + message].flatten.pack('C*'))

    sleep 0.1

    read_startup_response(username, password)
  end

  def read_startup_response(username, password)
    message_code, message_len = @socket.recv(5).unpack("al>")
    while message_code == 'R'
      auth_code = @socket.recv(4).unpack('l>').pop
      case auth_code
      when 5 # md5
        salt = @socket.recv(4).unpack('CCCC')
        send_md5_password_message(username, password, salt)
        message_code, message_len = @socket.recv(5).unpack("al>")
      when 0 # trust
        break
      end
    end
    loop do
      message_code, message_len = @socket.recv(5).unpack("al>")
      if message_code == 'Z'
        @socket.recv(1).unpack("a") # most likely I
        break # We are good to go
      end
      if message_code == 'S'
        actual_message = @socket.recv(message_len - 4).unpack("C*")
        k,v = actual_message.pack('U*').split(/\x00/)
        @parameters[k] = v
      end
      if message_code == 'K'
        process_id, secret_key = @socket.recv(message_len - 4).unpack("l>l>")
        @parameters["process_id"] = process_id
        @parameters["secret_key"] = secret_key
      end
    end
    return @parameters
  end

  def cancel_query
    socket = TCPSocket.new @host, @port
    process_key = @parameters["process_id"]
    secret_key =  @parameters["secret_key"]
    message = []
    message << [16].pack('l>').unpack('CCCC') # 4
    message << [80877102].pack('l>').unpack('CCCC') # 4
    message << [process_key.to_i].pack('l>').unpack('CCCC') # 4
    message << [secret_key.to_i].pack('l>').unpack('CCCC') # 4
    message.flatten!
    socket.write(message.flatten.pack('C*'))
    socket.close
    log "[F] Sent CancelRequest message"
  end

  def read_from_server()
    output_messages = []
    retry_count = 0
    message_code = nil
    message_len = 0
    loop do
      begin
        message_code, message_len = @socket.recv_nonblock(5).unpack("al>")
      rescue IO::WaitReadable
        return output_messages if retry_count > 50

        retry_count += 1
        sleep(0.01)
        next
      end
      message = {
        code: message_code,
        len: message_len,
        bytes: []
      }
      log "[B] #{BACKEND_MESSAGE_CODES[message_code] || ('UnknownMessage(' + message_code + ')')}"

      actual_message_length = message_len - 4
      if actual_message_length > 0
        message[:bytes] = @socket.recv(message_len - 4).unpack("C*")
        log "\t#{message[:bytes].join(",")}"
        log "\t#{message[:bytes].map(&:chr).join(" ")}"
      end
      output_messages << message
      return output_messages if message_code == 'Z'
    end
  end

  def log(msg)
    return unless @verbose

    puts msg
  end

  def close
    @socket.close
  end
end
