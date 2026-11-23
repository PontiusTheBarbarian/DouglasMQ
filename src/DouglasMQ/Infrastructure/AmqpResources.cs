using System.Threading.Tasks;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Graceful shutdown for RabbitMQ channels and connections: close (so the broker sees a clean
	/// disconnect) if still open, then always dispose.
	/// </summary>
	internal static class AmqpResources
	{
		/// <summary>Closes <paramref name="channel"/> if it is open, then disposes it. Does nothing for <c>null</c>.</summary>
		public static async ValueTask CloseAndDisposeAsync(IChannel? channel)
		{
			if (channel is null)
			{
				return;
			}

			try
			{
				if (channel.IsOpen)
				{
					await channel.CloseAsync();
				}
			}
			finally
			{
				await channel.DisposeAsync();
			}
		}

		/// <summary>Closes <paramref name="connection"/> if it is open, then disposes it. Does nothing for <c>null</c>.</summary>
		public static async ValueTask CloseAndDisposeAsync(IConnection? connection)
		{
			if (connection is null)
			{
				return;
			}

			try
			{
				if (connection.IsOpen)
				{
					await connection.CloseAsync();
				}
			}
			finally
			{
				await connection.DisposeAsync();
			}
		}
	}
}
