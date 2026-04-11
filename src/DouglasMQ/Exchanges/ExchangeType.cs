using System.ComponentModel;

namespace DouglasMQ
{
	/// <summary>
	/// Represents the types of message exchanges used in a message broker system.
	/// Exchanges determine how messages are routed to one or more queues based on specific rules 
	/// such as routing keys, patterns, or headers.
	/// </summary>
	public enum ExchangeType
	{
		/// <summary>
		/// A direct exchange delivers messages to queues based on the message's routing key.
		/// </summary>
		[Description("direct")]
		direct,

		/// <summary>
		/// A topic exchange routes messages to queues based on pattern matching between the routing key and the queue's binding key.
		/// </summary>
		[Description("topic")]
		topic,

		/// <summary>
		/// A fanout exchange broadcasts messages to all bound queues, ignoring the routing key.
		/// </summary>
		[Description("fanout")]
		fanout,

		/// <summary>
		/// A headers exchange routes messages based on header attributes instead of a routing key.
		/// </summary>
		[Description("headers")]
		headers
	}
}
