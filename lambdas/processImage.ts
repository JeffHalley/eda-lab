import { SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event) => {
  console.log("Event:", JSON.stringify(event));

  for (const record of event.Records) {
    const recordBody = JSON.parse(record.body);
    const snsMessage = JSON.parse(recordBody.Message);

    if (snsMessage.Records) {
      for (const s3Message of snsMessage.Records) {
        const srcKey = decodeURIComponent(
          s3Message.s3.object.key.replace(/\+/g, " ")
        );

        // Determine file type
        const typeMatch = srcKey.match(/\.([^.]*)$/);
        if (!typeMatch) {
          console.log("Could not determine the image type:", srcKey);
          throw new Error("Could not determine the image type.");
        }

        const imageType = typeMatch[1].toLowerCase();
        if (imageType !== "jpeg" && imageType !== "png") {
          console.log("Unsupported image type:", imageType, "for file:", srcKey);
          throw new Error(`Unsupported image type: ${imageType}`);
        }

        // Store valid image file name in DynamoDB
        try {
          await ddbDocClient.send(
            new PutCommand({
              TableName: process.env.TABLE_NAME!,
              Item: { name: srcKey },
            })
          );
          console.log(`Stored valid image in DynamoDB: ${srcKey}`);
        } catch (err) {
          console.error("Error storing image in DynamoDB:", err);
          throw err; // Let SQS retry or DLQ handle it
        }
      }
    }
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
