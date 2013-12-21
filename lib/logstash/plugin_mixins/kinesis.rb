
# encoding: utf-8
require "logstash/config/mixin"

module LogStash::PluginMixins::Kinesis
	protected
	def aws_service_endpoint(region)
		{:region => region}
	end

	def validate_stream
		# will throw a AWS::Kinesis::Errors::ResourceNotFoundException if stream does not exist
		stream_info = @kinesis.client.describe_stream(:stream_name => @stream)[:stream_description]
		raise "Kinesis stream #{@stream} status is not active" unless %w(ACTIVE UPDATING).include? stream_info[:stream_status]
	end
end
