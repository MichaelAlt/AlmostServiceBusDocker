using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Net;
using System.Text;

namespace AlmostServiceBus.Tests.Management;

public class ManagementApiTopicTests : IAsyncLifetime
{
    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.ConfigureServices(services => services.AddRouting());
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();
        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private StringContent TopicXmlBody(string topicName)
    {
        var topic = new TopicEntity(topicName);
        var xml = AtomXmlWriter.WriteTopicEntry(topic);
        return new StringContent(xml, Encoding.UTF8, "application/atom+xml");
    }

    [Fact]
    public async Task CreateTopic_Returns201()
    {
        var response = await _client.PutAsync("/my-topic", TopicXmlBody("my-topic"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("TopicDescription", body);
        Assert.Contains("my-topic", body);
    }

    [Fact]
    public async Task GetTopic_Returns200()
    {
        await _client.PutAsync("/get-topic", TopicXmlBody("get-topic"));

        var response = await _client.GetAsync("/get-topic");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("TopicDescription", body);
        Assert.Contains("get-topic", body);
    }
}
