import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    //
    // Integration infrastructure
    //

    const imageProcessQueue = new sqs.Queue(this, "img-process-q", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    // second queue
    const mailerQ = new sqs.Queue(this, "mailer-q", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    });

    //
    // Lambda functions
    //

    const processImageFn = new lambdanode.NodejsFunction(
      this,
      "ProcessImage",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/processImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
      }
    );

    // mailer lambda
    const mailerFn = new lambdanode.NodejsFunction(this, "mailer", {
      runtime: lambda.Runtime.NODEJS_16_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/mailer.ts`,
    });

    //
    // S3 - SNS
    //

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    //
    // SNS - SQS queues
    //

    newImageTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue)
    );

    // SNS subscriber for mailer queue
    newImageTopic.addSubscription(
      new subs.SqsSubscription(mailerQ)
    );

    //
    // SQS - Lambda event sources
    //

    const newImageEventSource = new events.SqsEventSource(
      imageProcessQueue,
      {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(5),
      }
    );

    processImageFn.addEventSource(newImageEventSource);

    // event source for mailer lambda
    const newImageMailEventSource = new events.SqsEventSource(
      mailerQ,
      {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(5),
      }
    );

    mailerFn.addEventSource(newImageMailEventSource);

    //
    // Permissions
    //

    imagesBucket.grantRead(processImageFn);

    // mailer SES permissions
    mailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    //
    // Output
    //

    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });
  }
}
