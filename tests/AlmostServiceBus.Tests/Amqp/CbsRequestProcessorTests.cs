using Amqp;
using Amqp.Framing;
using AlmostServiceBus.Core.Amqp;

namespace AlmostServiceBus.Tests.Amqp;

public class CbsRequestProcessorTests
{
    [Fact]
    public void Credit_ReturnsPositiveValue()
    {
        var processor = new CbsRequestProcessor();
        Assert.True(processor.Credit > 0);
    }
}
