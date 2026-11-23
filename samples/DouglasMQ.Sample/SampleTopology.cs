using System.Diagnostics;

namespace DouglasMQ.Sample;

public static class SampleTopology
{
	public const string ServiceName = "douglasmq-sample";

	public const string OrdersExchange = "sample.orders";
	public const string OrdersQueue = "sample.order-processing";

	public const string ShippingExchange = "sample.shipping";
	public const string ShippingQueue = "sample.shipping";

	/// <summary>
	/// The sample's own activity source, used to add application spans inside handlers.
	/// </summary>
	public static readonly ActivitySource ActivitySource = new(ServiceName);
}
