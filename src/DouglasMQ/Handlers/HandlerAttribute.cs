using System;

namespace DouglasMQ
{
	/// <summary>
	///
	/// </summary>
	[AttributeUsage(AttributeTargets.Class,
		AllowMultiple = false,
		Inherited = true)]
	public class HandlerAttribute : Attribute
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="exchangeType"></param>
		/// <param name="exchangeName"></param>
		/// <param name="queueName"></param>
		/// <param name="routingKey"></param>
		public HandlerAttribute(
			ExchangeType exchangeType,
			string exchangeName,
			string queueName,
			string routingKey = "*")
		{
			ExchangeType = exchangeType.ToString().ToLowerInvariant();
			ExchangeName = exchangeName;
			QueueName = queueName;
			RoutingKey = routingKey;
			ConsumerTag = $"{exchangeName}.{queueName}.{routingKey}";
		}

		/// <summary>
		/// 
		/// </summary>
		public string ExchangeType { get; }
		/// <summary>
		/// 
		/// </summary>
		public string ExchangeName { get; }
		/// <summary>
		/// 
		/// </summary>
		public string QueueName { get; }
		/// <summary>
		/// 
		/// </summary>
		public string RoutingKey { get; }
		/// <summary>
		/// 
		/// </summary>
		public string ConsumerTag { get; }
	}
}
