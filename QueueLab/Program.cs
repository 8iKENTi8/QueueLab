using Microsoft.Extensions.Options;
using QueueLab.Messaging;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.Configure<RabbitMqSettings>(builder.Configuration.GetSection("RabbitMq"));

builder.Services.AddSingleton(sp =>
{
    var settings = sp.GetRequiredService<IOptions<RabbitMqSettings>>().Value;

    return new ConnectionFactory
    {
        HostName = settings.HostName,
        Port = settings.Port,
        UserName = settings.UserName,
        Password = settings.Password
    };
});

builder.Services.AddSingleton<MessagePublisher>();
builder.Services.AddHostedService<MessageConsumer>();

builder.Services.AddSingleton<IHostedService>(sp =>
{
    var options = sp.GetRequiredService<IOptions<RabbitMqSettings>>();

    return new BroadcastConsumer(
        sp.GetRequiredService<ConnectionFactory>(),
        options,
        sp.GetRequiredService<ILogger<BroadcastConsumer>>(),
        options.Value.BroadcastQueueA);
});

builder.Services.AddSingleton<IHostedService>(sp =>
{
    var options = sp.GetRequiredService<IOptions<RabbitMqSettings>>();

    return new BroadcastConsumer(
        sp.GetRequiredService<ConnectionFactory>(),
        options,
        sp.GetRequiredService<ILogger<BroadcastConsumer>>(),
        options.Value.BroadcastQueueB);
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
