# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

require "logstash/plugin_mixins/aws_config"
require "logstash/plugin_mixins/kinesis"

# INFORMATION:

# This plugin was created for store the logstash's events into Amazon Kinesis.
# To use it you need AWS credentials with proper permissions and a kinesis stream.
#
# USAGE:

# Example config:

# output {
#    kinesis{ 
#      access_key_id => "key_id"                (required)
#      secret_access_key => "some_access_key"   (required)
#      region => "eu-west-1"           (required)
#      stream => "big-log"                      (required)
#      partition_key => "%{message}"            (required)
#    }
# }
#
# The Kinesis specific configurations are:
# stream - The Kinesis stream name
# partition_key - This will be hashed and used for selecting which shard the messsage is sent to.
# If the partition key does not vary between messages then throughput is limited to one shard.

class LogStash::Inputs::Kinesis < LogStash::Inputs::Base
	include LogStash::PluginMixins::AwsConfig
	include LogStash::PluginMixins::Kinesis

	config_name "kinesis"
	milestone 1

	# The name of the Kinesis stream to use.
	# The stream should be created before using logstash with it using Kinesis API or console.
	config :stream, :validate => :string, :required => true

	config :shard_id, :validate => :string, :required => true, :default => nil
	# Where to start in the stream. Valid values are TREAM_HORIZON, LATEST or a sequence number
	config :start_from, :validate => :string, :default => "LATEST"
	config :batch_size, :validate => :number, :required => false, :default => 100
	config :checkpoint_file, :validate => :string, :required => false, :default => nil
	
	public
	def register
		require "aws-sdk"
		@kinesis = AWS::Kinesis.new(aws_options_hash)
		validate_stream
		@backoff_time = 1
	end

	def run(output_queue)
		@starting_sequence_number = case @start_from
			when "LATEST", "TRIM_HORIZON"
				load_from_checkpoint
			when /\d+/
				@start_from
			end
		@start_from = "AFTER_SEQUENCE_NUMBER" if @starting_sequence_number
		@logger.debug("Setting starting sequence number and shard iterator type", :shard_iterator_type => @start_from, :starting_sequence_number => @starting_sequence_number)
		# get an initial shard iterator
		opts = {
			:shard_iterator_type => @start_from,
			:shard_id => @shard_id,
			:stream_name => @stream
		}
		opts[:starting_sequence_number] = @starting_sequence_number if @starting_sequence_number
		shard_iterator = @kinesis.client.get_shard_iterator(opts)[:shard_iterator]
		@logger.debug("Got initial shard iterator", :shard_iterator => shard_iterator)
		@current_sequence_number = ""

		loop do
			begin
				res = @kinesis.client.get_records(:shard_iterator => shard_iterator, :limit => @batch_size)
				@logger.debug("Got a reply from AWS Kinesis", :response => res)
				if res[:records].any?
					res[:records].each do |record|
						@codec.decode(record[:data]) do |event|
							decorate(event)
							event["sequence_number"] = record[:sequence_number]
							output_queue << event
						end
						# since we're already iterating, set the max sequence number as the current sequence number
						@current_sequence_number = record[:sequence_number] if record[:sequence_number] > @current_sequence_number
					end
					checkpoint(@current_sequence_number)
					@backoff_time = 1 # reset the backoff time
				else
					# wait a bit, so we don't go haywire with resources
					backoff
				end
				shard_iterator = res[:next_shard_iterator]
			rescue AWS::Kinesis::Errors::ExpiredIteratorException => e
				@logger.warn("Shard iterator expired. Fetching new shard iterator", :shard_iterator => shard_iterator)
				shard_iterator = shard_iterator = @kinesis.client.get_shard_iterator(
					:shard_iterator_type => "AFTER_SEQUENCE_NUMBER",
					:shard_id => @shard_id,
					:stream_name => @stream,
					:sequence_number => @current_sequence_number
				)[:shard_iterator]
			end
		end
	end

	def teardown
		@checkpoint.close if @checkpoint and not @checkpoint.closed?
		super
	end

	private
	def backoff
		sleep(@backoff_time)
		@backoff_time += 1 unless @backoff_time == 5 # sleep at most 5 seconds
	end

	def checkpoint(sequence_number)
		if @checkpoint_file
			unless @checkpoint
				@logger.debug("Opening checkpoint file for writing", :checkpoint_file => @checkpoint_file)
				@checkpoint = File.open(@checkpoint_file, 'w')
			end
			@checkpoint.seek(0)
			@checkpoint.write({:shard_id => @shard_id, :stream => @stream, :sequence_number => sequence_number}.to_json)
			@logger.debug("checkpointed", :sequence_number => sequence_number)
		end
	end

	CHECKPOINT_KEYS = %w(shard_id sequence_number stream)
	def load_from_checkpoint
		if @checkpoint_file and File.exists?(@checkpoint_file)
			@logger.debug("Loading from checkpoint file", :checkpoint_file => @checkpoint_file)
			checkpoint_data = JSON.load(File.read(@checkpoint_file))
			if checkpoint_data.is_a?(Hash) and CHECKPOINT_KEYS.all?{|k| checkpoint_data.keys.include? k }
				raise "Checkpoint data does not match stream or shard" unless checkpoint_data["shard_id"] == @shard_id and checkpoint_data["stream"]
				@logger.debug("Loaded checkpoint sequence number", :sequence_number => checkpoint_data["sequence_number"])
				checkpoint_data["sequence_number"]
			end
		end
	end
end
