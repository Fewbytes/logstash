# encoding: utf-8
require "logstash/outputs/base"
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

class LogStash::Outputs::Kinesis < LogStash::Outputs::Base
	include LogStash::PluginMixins::AwsConfig
	include LogStash::PluginMixins::Kinesis

	config_name "kinesis"
	milestone 1

	# The name of the Kinesis stream to use.
	# The stream should be created before using logstash with it using Kinesis API or console.
	config :stream, :validate => :string, :required => true

	# The key to use for partitioning events for shards - the partition key will be processed by sprintf per event, hashed then used to select a shard to send the event to.
	# Note that if the partition key does not vary between events messages will be sent to the same shard limiting throughput.
	# On the other hand, consumers process a shard and will get all messages for a partition key. In other words, give this some thought.
	config :partition_key, :validate => :string, :default => "%{message}", :required => false

 	public
	def register
		require "aws-sdk"
		@kinesis = AWS::Kinesis.new(aws_options_hash)
		validate_stream
	end
 
	def receive(event)
		return unless output?(event)
		@codec.on_event do |data|
			partition_key = event.sprintf(@partition_key)
			@logger.debug("Sending data to kinesis", :partition_key => partition_key, :data => data)
			res = @kinesis.client.put_record(
				:stream_name => @stream,
				:data => data,
				:partition_key => partition_key
			)
			@logger.debug("Sent event to kinesis", :response => res)
		end
		@codec.encode(event)
	end
end
