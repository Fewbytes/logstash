require "logstash/inputs/base"
require "logstash/namespace"
require "timeout"

# Read events over a 0MQ SUB socket.
#
# You need to have the 0mq 2.1.x library installed to be able to use
# this input plugin.
#
# The default settings will create a subscriber binding to tcp://127.0.0.1:2120 
# waiting for connecting publishers.
#
class LogStash::Inputs::ZeroMQ < LogStash::Inputs::Base

  config_name "zeromq"
  plugin_status "beta"

  # 0mq socket address to connect or bind
  # Please note that `inproc://` will not work with logstash
  # as each we use a context per thread.
  # By default, inputs bind/listen
  # and outputs connect
  config :address, :validate => :array, :default => ["tcp://*:2120"]

  # 0mq topology
  # The default logstash topologies work as follows:
  # * pushpull - inputs are pull, outputs are push
  # * pubsub - inputs are subscribers, outputs are publishers
  # * pair - inputs are clients, inputs are servers
  #
  # If the predefined topology flows don't work for you,
  # you can change the 'mode' setting
  # TODO (lusis) add req/rep MAYBE
  # TODO (lusis) add router/dealer
  config :topology, :validate => ["pushpull", "pubsub", "pair"]

  # 0mq topic
  # This is used for the 'pubsub' topology only
  # On inputs, this allows you to filter messages by topic
  # On outputs, this allows you to tag a message for routing
  # NOTE: ZeroMQ does subscriber side filtering.
  # NOTE: All topics have an implicit wildcard at the end
  # You can specify multiple topics here
  config :topic, :validate => :array

  # mode
  # server mode binds/listens
  # client mode connects
  config :mode, :validate => ["server", "client"], :default => "server"

  # sender
  # overrides the sender to 
  # set the source of the event
  # default is "zmq+topology://type/"
  config :sender, :validate => :string

  # 0mq socket options
  # This exposes zmq_setsockopt
  # for advanced tuning
  # see http://api.zeromq.org/2-1:zmq-setsockopt for details
  #
  # This is where you would set values like:
  # ZMQ::HWM - high water mark
  # ZMQ::IDENTITY - named queues
  # ZMQ::SWAP_SIZE - space for disk overflow
  #
  # example: sockopt => ["ZMQ::HWM", 50, "ZMQ::IDENTITY", "my_named_queue"]
  config :sockopt, :validate => :hash

  public
  def register
    require "ffi-rzmq"
    require "logstash/util/zeromq"
    @format ||= "json_event"
    self.class.send(:include, LogStash::Util::ZeroMQ)

    case @topology
    when "pair"
      @zmq_const = ZMQ::PAIR 
    when "pushpull"
      @zmq_const = ZMQ::PULL
    when "pubsub"
      @zmq_const = ZMQ::SUB
    end # case socket_type

    setup
    
    if @topology == "pubsub"
      if @topic.nil?
        @logger.debug("ZMQ - No topic provided. Subscribing to all messages")
        error_check(@zsocket.setsockopt(ZMQ::SUBSCRIBE, ""),
      "while setting ZMQ::SUBSCRIBE")
      else
        @topic.each do |t|
          @logger.debug("ZMQ subscribing to topic: #{t}")
          error_check(@zsocket.setsockopt(ZMQ::SUBSCRIBE, t),
        "while setting ZMQ::SUBSCRIBE == #{t}")
        end
      end
    end

  end # def register

  def teardown
    error_check(@zsocket.close, "while closing the zmq socket")
  end # def teardown

  def server?
    @mode == "server"
  end # def server?

  def run(output_queue)
    begin
      loop do
        # Here's the unified receiver
        # Get the first part as the msg
        m1 = ''
        rc = @zsocket.recv_string(m1)
        error_check(rc, "in recv_string")
        @logger.debug("ZMQ receiving", :event => m1)
        msg = m1
        # If we have more parts, we'll eat the first as the topic
        # and set the message to the second part
        if @zsocket.more_parts?
          @logger.debug("Multipart message detected. Setting @message to second part. First part was: #{m1}")
          m2 = ''
          rc2 = @zsocket.recv_string(m2)
          error_check(rc2, "in recv_string")
          @logger.debug("ZMQ receiving", :event => m2)
          msg = m2
        end

        @sender ||= "zmq+#{@topology}://#{@type}/"
        msg_array = Array.new
        rc = @zsocket.recv_strings(msg_array)
        error_check(rc, "in recv_strings")
        @logger.debug("0mq: receiving", :event => msg_array)
        if msg_array.count >1 and @zmq_const == ZMQ::SUB
          e = self.to_event(msg_array[1..-1].join("\n"), @source)
          e['@zeromq_topic'] = msg_array.first
        else
          e = self.to_event(msg_array.first, @sender)
        end
        if e
          output_queue << e
        end
      end
    rescue => e
      @logger.debug("ZMQ Error", :subscriber => @zsocket,
                    :exception => e, :backtrace => e.backtrace)
    rescue Timeout::Error
      @logger.debug("Read timeout", subscriber => @zsocket)
    end # begin
  end # def run

  private
  def build_source_string
    id = @address.first.clone
  end
end # class LogStash::Inputs::ZeroMQ
