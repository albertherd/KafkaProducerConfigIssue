using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

try
{
    var builder = WebApplication.CreateBuilder(args);

    // Add services to the container.
    // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();

    var app = builder.Build();

    // Configure the HTTP request pipeline.
    if (app.Environment.IsDevelopment())
    {
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.MapGet("/main", async () =>
    {
        ProducerConfig cfg = new ProducerConfig()
        {
            BootstrapServers = "non-existant:12345"
        };

        SchemaRegistryConfig src = new SchemaRegistryConfig()
        {
            Url = "non-existant:23456"
        };

        var producer = BuildProducer(cfg, src);

        await producer.ProduceAsync("topic", new Message<string, string>
        {
            Key = "key",
            Value = "message"
        });
    })
    .WithName("main");

    app.Run();
}
catch (Exception ex)
{
    Console.WriteLine(ex.ToString());
}


IProducer<string, string> BuildProducer(ProducerConfig producerConfig, SchemaRegistryConfig schemaRegistryConfig)
{
    var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
    return new ProducerBuilder<string, string>(producerConfig)
        .SetValueSerializer(new JsonSerializer<string>(schemaRegistry))
        .SetLogHandler((_, message) => Console.WriteLine("I am a log"))
        .SetErrorHandler((_, error) => Console.WriteLine("I am an error"))
        .Build();
}