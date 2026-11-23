using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class TopologyTests
	{
		[Theory]
		[InlineData(ExchangeType.direct, "direct")]
		[InlineData(ExchangeType.topic, "topic")]
		[InlineData(ExchangeType.fanout, "fanout")]
		[InlineData(ExchangeType.headers, "headers")]
		public void ToAmqpName_UsesProtocolNames(ExchangeType exchangeType, string expected)
		{
			Assert.Equal(expected, Topology.ToAmqpName(exchangeType));
		}

		[Fact]
		public async Task DeclareConsumerQueue_DeadLettersToTheDeclaredDeadLetterExchange()
		{
			// The queue's x-dead-letter-* arguments and the DLX declaration must agree, or rejected
			// messages are silently dropped.
			var channel = new Mock<IChannel>();
			IDictionary<string, object?>? queueArguments = null;
			channel
				.Setup(c => c.QueueDeclareAsync("orders", true, false, false, It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Callback<string, bool, bool, bool, IDictionary<string, object?>, bool, bool, CancellationToken>((_, _, _, _, args, _, _, _) => queueArguments = args)
				.ReturnsAsync(new QueueDeclareOk("orders", 0, 0));

			await Topology.DeclareConsumerQueueAsync(channel.Object, new HandlerAttribute(ExchangeType.topic, "orders.exchange", "orders", "order.*"), CancellationToken.None);

			channel.Verify(c => c.ExchangeDeclareAsync("orders.exchange", "topic", true, false, It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()));
			channel.Verify(c => c.QueueBindAsync("orders", "orders.exchange", "order.*", It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()));
			Assert.NotNull(queueArguments);
			Assert.Equal(Topology.DeadLetterExchange, queueArguments!["x-dead-letter-exchange"]);
			Assert.Equal(Topology.DeadLetterRoutingKey, queueArguments["x-dead-letter-routing-key"]);
		}

		[Fact]
		public async Task DeclareDeadLetter_BindsQueueToExchangeWithTheSharedRoutingKey()
		{
			var channel = new Mock<IChannel>();

			await Topology.DeclareDeadLetterAsync(channel.Object, CancellationToken.None);

			channel.Verify(c => c.ExchangeDeclareAsync(Topology.DeadLetterExchange, "direct", true, false, It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()));
			channel.Verify(c => c.QueueBindAsync(Topology.DeadLetterQueue, Topology.DeadLetterExchange, Topology.DeadLetterRoutingKey, It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()));
		}
	}
}
