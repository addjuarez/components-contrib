resource "aws_sns_topic" "testTopic" {
  name = "testTopic"
}

resource "aws_sns_topic" "multiTopic1" {
  name = "multiTopic1"
}

resource "aws_sns_topic" "multiTopic2" {
  name = "multiTopic2"
}

resource "aws_sqs_queue" "testQueue" {
  name = "testQueue"
}

resource "aws_sns_topic_subscription" "multiTopic1_testQueue" {
  topic_arn = aws_sns_topic.multiTopic1.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sns_topic_subscription" "multiTopic2_testQueue" {
  topic_arn = aws_sns_topic.multiTopic1.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sns_topic_subscription" "testTopic_testQueue" {
  topic_arn = aws_sns_topic.multiTopic1.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}