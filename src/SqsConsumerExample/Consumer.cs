using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System.Text.Json;

namespace SqsConsumerExample
{
    public class Consumer : IHostedService
    {
        private readonly AmazonSQSClient _sqsClient;
        private readonly string _queueUrl;

        public Consumer(IConfiguration configuration)
        {
            var awsSettings = configuration.GetSection("AwsSettings").Get<AwsSettings>();
            _queueUrl = awsSettings.QueueUrl;
            _sqsClient = new AmazonSQSClient(awsSettings.AccessKey, awsSettings.SecretKey, Amazon.RegionEndpoint.GetBySystemName(awsSettings.Region));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 10
            };

            var consumeCancellationToken = new CancellationTokenSource();

            Task.Run(async () =>
            {
                while (!consumeCancellationToken.IsCancellationRequested)
                {
                    var receiveMessageResponse = await _sqsClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);

                    foreach (var message in receiveMessageResponse.Messages)
                    {
                        var userDto = JsonSerializer.Deserialize<UserDto>(message.Body);
                        Console.WriteLine($"Received message with ID: {userDto.id.ToString()}, name: {userDto.name}, age: {userDto.age}");

                        // Delete the message from the queue after processing it to prevent it from being received again.
                        var deleteRequest = new DeleteMessageRequest
                        {
                            QueueUrl = _queueUrl,
                            ReceiptHandle = message.ReceiptHandle
                        };
                        var deleteResult = await _sqsClient.DeleteMessageAsync(deleteRequest);
                        Console.WriteLine($"Message with ID: {message.MessageId} deleted with success with status code {deleteResult.HttpStatusCode}!");
                    }
                }
            }, cancellationToken);
            

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _sqsClient.Dispose();
            return Task.CompletedTask;
        }
    }
}
