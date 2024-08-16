using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

ConnectionFactory factory = new();
factory.Uri = new("amqps://eiasajcb:pmj-eOwUiKVZpkyWfz294pr48-FiV1Wq@shark.rmq.cloudamqp.com/eiasajcb");


using IConnection connection = factory.CreateConnection();
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
	// Consumer üzerinde queue Publisher ile aynı şekilde oluşturulmalıdır.
	channel.QueueDeclare(queue: "example-queue", exclusive: false);
	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: "example-queue", true, consumer);

	consumer.Received += (sender, e) =>
	{
		// Kuyruktaki verilerin işlendiği yerdir.
		// Mesaj, e.Body.Span ya da e.Body.ToArray() denilerek alınabilir.
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
void DirectExchange()
{
	channel.ExchangeDeclare(exchange: "direct-example", type: ExchangeType.Direct);

	string queueName = channel.QueueDeclare().QueueName;
	channel.QueueBind(queue: queueName, exchange: "direct-example", routingKey: "direct-example-queue");

	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: queueName, autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
void FanoutExchange()
{
	channel.ExchangeDeclare(exchange: "fanout-example", type: ExchangeType.Fanout);

	Console.Write("Kuyruk Adını Giriniz : ");
	string queueName = Console.ReadLine();

	channel.QueueDeclare(queue: queueName, exclusive: false);
	channel.QueueBind(queue: queueName, exchange: "fanout-example", routingKey: "");

	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: queueName, autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};

}
void TopicExchange()
{
	channel.ExchangeDeclare(exchange: "topic-example", type: ExchangeType.Topic);

	Console.Write("Dinlenecek Topic Formatı Giriniz : ");
	string topic = Console.ReadLine();
	string queueName = channel.QueueDeclare().QueueName;
	channel.QueueBind(queue: queueName, exchange: "topic-example", routingKey: topic);

	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: queueName, autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
void HeaderExchange()
{
	channel.ExchangeDeclare(exchange: "header-example", type: ExchangeType.Headers);

	Console.Write("Dinlenecek Header Değerini Giriniz : ");
	string header = Console.ReadLine();
	string queueName = channel.QueueDeclare().QueueName;
	channel.QueueBind(queue: queueName, exchange: "header-example", routingKey: "", new Dictionary<string, object> { ["Key"] = header });

	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: queueName, autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
async Task RoundRobin()
{
	channel.QueueDeclare(queue: "round-robin-queue", exclusive: false);
	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: "round-robin-queue", autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
void Acknowledgement()
{
	channel.QueueDeclare(queue: "acknowledgement-queue", exclusive: false);
	EventingBasicConsumer consumer = new(channel);
	// autoAck : Otomatik olarak kuyruktan silinsin mi?
	channel.BasicConsume(queue: "acknowledgement-queue", autoAck: false, consumer);

	consumer.Received += (sender, e) =>
	{
		Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
		// multiple : İlgili mesaj ve ondan önce onaylanmamış tüm mesajlar onaylansın mı?
		 channel.BasicAck(e.DeliveryTag, multiple: false);

		 // requeue : Mesaj tekrardan kuyruğa eklensin mi?
		// channel.BasicReject(e.DeliveryTag,requeue: true);
	};
}
async Task FairDispatch()
{
	channel.QueueDeclare(queue: "fairdispatch-queue", exclusive: false);
	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: "fairdispatch-queue", autoAck: true, consumer);
	// prefetchSize : Bir consumer tarafından alınabilecek max mesaj boyutu (byte)
	// prefetchCount : Bir consumer tarafından aynı anda işleme alınabilecek mesaj sayısı
	// global : Yapılan konfigürasyonun sadece bu consumer için mi tümü için mi olduğunu belirler
	channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
	consumer.Received += async (sender, e) =>
	{
		//Task.Run(async () =>
		//{
		//    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));

		//    Random random = new Random();
		//    int delayMilliseconds = random.Next(1, 5000); 
		//    await Task.Delay(delayMilliseconds);
		//}).GetAwaiter().GetResult();
		//Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
	};
}
void ResponseQueue()
{
	channel.QueueDeclare(queue: "response-queue", exclusive: false, autoDelete: false);
	EventingBasicConsumer consumer = new(channel);
	channel.BasicConsume(queue: "response-queue", autoAck: true, consumer);
	consumer.Received += (sender, e) =>
	{
		string message = Encoding.UTF8.GetString(e.Body.Span);
		Console.WriteLine(message);
		//....

		IBasicProperties basicProperties = channel.CreateBasicProperties();
		basicProperties.CorrelationId = e.BasicProperties.CorrelationId;
		channel.BasicPublish(exchange: "", routingKey: e.BasicProperties.ReplyTo, basicProperties: basicProperties, body: Encoding.UTF8.GetBytes("İşlemler Başarılı. Mesaj : " + message));
	};
}

Console.Read();