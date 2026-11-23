namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Attribute names used on DouglasMQ spans. Standard names follow the OpenTelemetry messaging
	/// semantic conventions; <c>douglasmq.*</c> names are library specific.
	/// </summary>
	internal static class MessagingTags
	{
		public const string System = "messaging.system";
		public const string SystemValue = "rabbitmq";
		public const string OperationType = "messaging.operation.type";
		public const string DestinationName = "messaging.destination.name";
		public const string RoutingKey = "messaging.rabbitmq.destination.routing_key";
		public const string MessageId = "messaging.message.id";
		public const string ConversationId = "messaging.message.conversation_id";
		public const string ConsumerGroup = "messaging.consumer.group.name";
		public const string ErrorType = "error.type";

		public const string MessageType = "douglasmq.message.type";
		public const string Handler = "douglasmq.handler";
		public const string Attempt = "douglasmq.attempt";
	}
}
