using Amqp;
using Amqp.Framing;
using AzureServiceBusEmulator.Core.Amqp;

namespace AzureServiceBusEmulator.Tests.Amqp;

public class CbsRequestProcessorTests
{
    [Fact]
    public void Credit_ReturnsPositiveValue()
    {
        var processor = new CbsRequestProcessor();
        Assert.True(processor.Credit > 0);
    }
}
