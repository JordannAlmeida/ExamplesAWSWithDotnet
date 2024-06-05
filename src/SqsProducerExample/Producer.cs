using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System.Text.Json;


namespace SqsProducerExample
{
    public class Producer : IHostedService
    {
        private readonly AmazonSQSClient _sqsClient;
        private readonly string _queueUrl;

        public Producer(IConfiguration configuration)
        {
            var awsSettings = configuration.GetSection("AwsSettings").Get<AwsSettings>();
            _queueUrl = awsSettings.QueueUrl;
            _sqsClient = new AmazonSQSClient(awsSettings.AccessKey, awsSettings.SecretKey, Amazon.RegionEndpoint.GetBySystemName(awsSettings.Region));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var user = new UserDto(Guid.NewGuid(), "Maria", 25);
            var json = JsonSerializer.Serialize(user);

            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = _queueUrl,
                MessageBody = json,
                MessageGroupId = user.id.ToString(),
                MessageDeduplicationId = $"{user.id}{(DateTime.Now.Ticks)}"
            };

            _sqsClient.SendMessageAsync(sendMessageRequest, cancellationToken).Wait(cancellationToken);

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _sqsClient.Dispose();
            return Task.CompletedTask;
        }
    }
}
