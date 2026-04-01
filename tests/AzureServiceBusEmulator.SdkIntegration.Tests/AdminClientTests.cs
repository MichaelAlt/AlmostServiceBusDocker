using System.Net;
using System.Text;
using AzureServiceBusEmulator.TestHost;

namespace AzureServiceBusEmulator.SdkIntegration.Tests;

public class AdminClientTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private HttpClient _httpClient = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{_fixture.HttpPort}")
        };
        _httpClient.DefaultRequestHeaders.Host = $"{_fixture.Namespace}.servicebus.windows.net";
    }

    public async Task DisposeAsync()
    {
        _httpClient.Dispose();
        await _fixture.DisposeAsync();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static StringContent AtomXml(string descriptionXml) =>
        new(
            $"<entry xmlns=\"http://www.w3.org/2005/Atom\">" +
            $"<content type=\"application/xml\">{descriptionXml}</content>" +
            $"</entry>",
            Encoding.UTF8,
            "application/atom+xml");

    private static StringContent QueueBody(string? lockDuration = null)
    {
        var ld = lockDuration ?? "PT30S";
        return AtomXml(
            "<QueueDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\" " +
            "xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            $"<LockDuration>{ld}</LockDuration>" +
            "<MaxSizeInMegabytes>1024</MaxSizeInMegabytes>" +
            "<RequiresSession>false</RequiresSession>" +
            "<DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive>" +
            "<DeadLetteringOnMessageExpiration>false</DeadLetteringOnMessageExpiration>" +
            "<MaxDeliveryCount>10</MaxDeliveryCount>" +
            "<EnableBatchedOperations>true</EnableBatchedOperations>" +
            "</QueueDescription>");
    }

    private static StringContent TopicBody()
    {
        return AtomXml(
            "<TopicDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\" " +
            "xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            "<DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive>" +
            "<MaxSizeInMegabytes>1024</MaxSizeInMegabytes>" +
            "<EnableBatchedOperations>true</EnableBatchedOperations>" +
            "</TopicDescription>");
    }

    private static StringContent SubscriptionBody(string? forwardTo = null, int maxDeliveryCount = 10)
    {
        var forwardToElement = forwardTo is not null
            ? $"<ForwardTo>{forwardTo}</ForwardTo>"
            : "";
        return AtomXml(
            "<SubscriptionDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\" " +
            "xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            "<LockDuration>PT30S</LockDuration>" +
            "<RequiresSession>false</RequiresSession>" +
            "<DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive>" +
            "<DeadLetteringOnMessageExpiration>false</DeadLetteringOnMessageExpiration>" +
            $"<MaxDeliveryCount>{maxDeliveryCount}</MaxDeliveryCount>" +
            "<EnableBatchedOperations>true</EnableBatchedOperations>" +
            forwardToElement +
            "</SubscriptionDescription>");
    }

    private static StringContent SqlFilterRuleBody(string ruleName, string sqlExpression)
    {
        return AtomXml(
            "<RuleDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\" " +
            "xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            "<Filter i:type=\"SqlFilter\">" +
            $"<SqlExpression>{sqlExpression}</SqlExpression>" +
            "</Filter>" +
            "<Action i:type=\"EmptyRuleAction\" />" +
            $"<Name>{ruleName}</Name>" +
            "</RuleDescription>");
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateQueue_ThenGetQueue_RoundTrips()
    {
        // Create
        var createResponse = await _httpClient.PutAsync("/test-queue", QueueBody("PT1M"));
        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var createBody = await createResponse.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", createBody);
        Assert.Contains("test-queue", createBody);

        // Get
        var getResponse = await _httpClient.GetAsync("/test-queue");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var getBody = await getResponse.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", getBody);
        Assert.Contains("PT1M", getBody); // LockDuration should round-trip
    }

    [Fact]
    public async Task CreateTopicAndSubscription_Works()
    {
        // Create topic
        var topicResponse = await _httpClient.PutAsync("/my-topic", TopicBody());
        Assert.Equal(HttpStatusCode.Created, topicResponse.StatusCode);

        // Create subscription with ForwardTo
        var subResponse = await _httpClient.PutAsync(
            "/my-topic/Subscriptions/sub-1",
            SubscriptionBody(forwardTo: "some-queue"));
        Assert.Equal(HttpStatusCode.Created, subResponse.StatusCode);

        // Get subscription and verify ForwardTo
        var getResponse = await _httpClient.GetAsync("/my-topic/Subscriptions/sub-1");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var body = await getResponse.Content.ReadAsStringAsync();
        Assert.Contains("SubscriptionDescription", body);
        Assert.Contains("some-queue", body); // ForwardTo value
    }

    [Fact]
    public async Task GetNonexistentEntity_Returns404()
    {
        var response = await _httpClient.GetAsync("/nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("MessagingEntityNotFound", body);
    }

    [Fact]
    public async Task CreateSubscriptionWithRules_Works()
    {
        // Create topic and subscription
        await _httpClient.PutAsync("/rules-topic", TopicBody());
        await _httpClient.PutAsync("/rules-topic/Subscriptions/rules-sub", SubscriptionBody());

        // Create a SQL filter rule
        var ruleResponse = await _httpClient.PutAsync(
            "/rules-topic/Subscriptions/rules-sub/Rules/my-rule",
            SqlFilterRuleBody("my-rule", "color = 'blue'"));
        Assert.Equal(HttpStatusCode.Created, ruleResponse.StatusCode);

        // Get the rule and verify round-trip
        var getResponse = await _httpClient.GetAsync(
            "/rules-topic/Subscriptions/rules-sub/Rules/my-rule");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var body = await getResponse.Content.ReadAsStringAsync();
        Assert.Contains("RuleDescription", body);
        Assert.Contains("SqlFilter", body);
        Assert.Contains("color = 'blue'", body);
        Assert.Contains("my-rule", body);
    }

    [Fact]
    public async Task DeleteEntity_Works()
    {
        // Create queue
        var createResponse = await _httpClient.PutAsync("/delete-me", QueueBody());
        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        // Delete it
        var deleteResponse = await _httpClient.DeleteAsync("/delete-me");
        Assert.Equal(HttpStatusCode.OK, deleteResponse.StatusCode);

        // Verify it's gone
        var getResponse = await _httpClient.GetAsync("/delete-me");
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    [Fact]
    public async Task UpdateSubscription_WithIfMatch_Works()
    {
        // Create topic and subscription
        await _httpClient.PutAsync("/update-topic", TopicBody());
        await _httpClient.PutAsync(
            "/update-topic/Subscriptions/update-sub",
            SubscriptionBody(maxDeliveryCount: 10));

        // Update with If-Match header and changed MaxDeliveryCount
        var updateRequest = new HttpRequestMessage(HttpMethod.Put,
            "/update-topic/Subscriptions/update-sub")
        {
            Content = SubscriptionBody(maxDeliveryCount: 5)
        };
        updateRequest.Headers.Add("If-Match", "*");

        var updateResponse = await _httpClient.SendAsync(updateRequest);
        Assert.Equal(HttpStatusCode.OK, updateResponse.StatusCode);

        // Get and verify updated value
        var getResponse = await _httpClient.GetAsync("/update-topic/Subscriptions/update-sub");
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        var body = await getResponse.Content.ReadAsStringAsync();
        Assert.Contains("<MaxDeliveryCount", body);
        Assert.Contains("5", body);
    }
}
