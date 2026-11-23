namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Attribute names used on DouglasMQ spans and metrics. Standard names follow the
	/// OpenTelemetry messaging semantic conventions; <c>douglasmq.*</c> names are library specific.
	/// </summary>
	internal static class MessagingTags
	{
		public const string System = "messaging.system";
		public const string SystemValue = "rabbitmq";
		public const string OperationName = "messaging.operation.name";
		public const string OperationType = "messaging.operation.type";
		public const string DestinationName = "messaging.destination.name";
		public const string SubscriptionName = "messaging.destination.subscription.name";
		public const string RoutingKey = "messaging.rabbitmq.destination.routing_key";
		public const string DeliveryTag = "messaging.rabbitmq.message.delivery_tag";
		public const string Redelivered = "messaging.rabbitmq.message.redelivered";
		public const string MessageId = "messaging.message.id";
		public const string ConversationId = "messaging.message.conversation_id";
		public const string BodySize = "messaging.message.body.size";
		public const string ErrorType = "error.type";

		public const string MessageType = "douglasmq.message.type";
		public const string Handler = "douglasmq.handler";
		public const string Outcome = "douglasmq.outcome";
		public const string Attempt = "douglasmq.attempt";
		public const string RetryCount = "douglasmq.retry_count";
		public const string PublisherConfirms = "douglasmq.publisher_confirms";

		public const string OutcomeAck = "ack";
		public const string OutcomeDeadLettered = "dead_lettered";
		public const string OutcomeRequeued = "requeued";
	}
}
