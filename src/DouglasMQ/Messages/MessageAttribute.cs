using System;

namespace DouglasMQ
{
	/// <summary>
	///
	/// </summary>
	[AttributeUsage(AttributeTargets.Class,
		AllowMultiple = false,
		Inherited = true)]
	public sealed class MessageAttribute : Attribute
	{
		/// <summary>
		/// Initialises a new <see cref="MessageAttribute"/>.
		/// </summary>
		/// <param name="exchangeName">The name of the exchange this message is published to.</param>
		/// <param name="routingKey">The routing key used when publishing. Defaults to <c>"*"</c>.</param>
		public MessageAttribute(string exchangeName,
			string routingKey = "*")
		{
			ExchangeName = exchangeName;
			RoutingKey = routingKey;
		}
		/// <summary>
		/// 
		/// </summary>
		public string ExchangeName { get; }
		/// <summary>
		/// 
		/// </summary>
		public string RoutingKey { get; }
	}
}
