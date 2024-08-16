using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Reflection.PortableExecutable;
using System.Text;

ConnectionFactory factory = new();
factory.Uri = new("amqps://eiasajcb:pmj-eOwUiKVZpkyWfz294pr48-FiV1Wq@shark.rmq.cloudamqp.com/eiasajcb");


// Bağlantı aktifleştirme
using IConnection connection = factory.CreateConnection();

// Kanal oluşturma
IModel channel = connection.CreateModel();



//FirstQueue();
//DirectExchange();
//FanoutExchange();
//TopicExchange();
//HeaderExchange();
//RoundRobin();
//Acknowledgement();
//FairDispatch();
//ResponseQueue();



void FirstQueue()
{
	// durable : kuyruktaki mesajların kalıcı olarak kalsın mı?
	// exclusive : birden fazla bağlantı ile işlem gerçekleştirilebilir mi?
	// autoDelete : kuyruktaki tüm mesajlar tüketilince kuyruk silinsin mi?

	channel.QueueDeclare(queue: "example-queue", exclusive: false);
    // Queue' ya mesaj göndermek
    // RabbitMQ kuyruğa atacağı mesajları byte türünde kabul eder.

    byte[] message = Encoding.UTF8.GetBytes("Finekra");

    // Default olarak DirectExchange olarak işlem görür
    channel.BasicPublish(exchange: "", routingKey: "example-queue", body: message);
}
void DirectExchange()
{
    channel.ExchangeDeclare(exchange: "direct-example", type: ExchangeType.Direct);
    while (true)
    {
        Console.Write("Mesaj Giriniz : ");
        byte[] message = Encoding.UTF8.GetBytes(Console.ReadLine());
        channel.BasicPublish(exchange: "direct-example", routingKey: "direct-example-queue", body: message);
    }
}
async Task FanoutExchange()
{
    channel.ExchangeDeclare(exchange: "fanout-example", type: ExchangeType.Fanout);

    for (int i = 0; i < 100; i++)
    {
        await Task.Delay(90);
        Console.WriteLine($"Finekra {i}");
        channel.BasicPublish(exchange: "fanout-example", routingKey: string.Empty, body: Encoding.UTF8.GetBytes($"Finekra {i}"));
    }
}
void TopicExchange()
{
    channel.ExchangeDeclare(exchange: "topic-example", type: ExchangeType.Topic);

    for (int i = 0; i < 100; i++)
    {
        Console.WriteLine($"Finekra {i}");
        Console.Write("Topic Formatı Giriniz : ");
        string topic = Console.ReadLine();
        channel.BasicPublish(exchange: "topic-example", routingKey: topic, body: Encoding.UTF8.GetBytes($"Finekra {i}"));
    }
}
void HeaderExchange()
{
    channel.ExchangeDeclare(exchange: "header-example", type: ExchangeType.Headers);

    for (int i = 0; i < 100; i++)
    {
        Console.WriteLine($"Finekra {i}");
        Console.Write("Header Giriniz : ");
        string header = Console.ReadLine();

        IBasicProperties basicProperties = channel.CreateBasicProperties();

        basicProperties.Headers = new Dictionary<string, object>
        {
            ["Key"] = header
        };

        channel.BasicPublish(exchange: "header-example", routingKey: header, body: Encoding.UTF8.GetBytes($"Finekra {i}"), basicProperties: basicProperties);
    }
}
async Task RoundRobin()
{
    channel.QueueDeclare(queue: "round-robin-queue", exclusive: false);
    for (int i = 0; i < 100; i++)
    {
        await Task.Delay(90);
        channel.BasicPublish(exchange: "", routingKey: "round-robin-queue", body: Encoding.UTF8.GetBytes($"Finekra {i}"));
    }
}
async Task Acknowledgement()
{
    channel.QueueDeclare(queue: "acknowledgement-queue", exclusive: false);
    for (int i = 0; i < 100; i++)
    {
        await Task.Delay(90);
        channel.BasicPublish(exchange: "", routingKey: "acknowledgement-queue", body: Encoding.UTF8.GetBytes($"Finekra {i}"));
    }
}
async Task FairDispatch()
{
    channel.QueueDeclare(queue: "fairdispatch-queue", exclusive: false);
    for (int i = 0; i < 100; i++)
    {
        await Task.Delay(90);
        channel.BasicPublish(exchange: "", routingKey: "fairdispatch-queue", body: Encoding.UTF8.GetBytes($"Finekra {i}"));
    }
}
async Task ResponseQueue()
{
    channel.QueueDeclare(queue: "response-queue", exclusive: false, autoDelete: false);

    string queueName = channel.QueueDeclare().QueueName;
    string correlationId = Guid.NewGuid().ToString();

    IBasicProperties basicProperties = channel.CreateBasicProperties();

    basicProperties.CorrelationId = correlationId;
    basicProperties.ReplyTo = queueName;

    for (int i = 0; i < 10; i++)
        channel.BasicPublish(exchange: "", routingKey: "response-queue", body: Encoding.UTF8.GetBytes($"Finekra {i}"), basicProperties: basicProperties);

    EventingBasicConsumer consumer = new(channel);
    channel.BasicConsume(queue: queueName, autoAck: true, consumer);
    consumer.Received += (sender, e) =>
    {
        if (e.BasicProperties.CorrelationId == correlationId) 
            Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
    };
}

Console.Read();