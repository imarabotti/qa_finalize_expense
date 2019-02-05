const AWS = require('aws-sdk');

exports.handler = async(event) => {

    const message = JSON.parse(event.Records[0].body);

    const SQS = new AWS.SQS({ apiVersion: '2012-11-05', region: 'us-west-2' });
    const S3 = new AWS.S3({ apiVersion: '2006-03-01', region: 'us-west-2' });

    let s3Params = {
        Bucket: message.bucket,
        Key: message.ruta
    };

    let file;

    try {
        file = await S3.getObject(s3Params).promise();
    }
    catch (ex) {
        return 'Done';
    }

    let data = JSON.parse(file.Body.toString());

    let response;

    try {
        let attrParams = {
            QueueUrl: data.QueueUrl,
            AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed', 'ApproximateNumberOfMessagesNotVisible']
        };

        response = await SQS.getQueueAttributes(attrParams).promise();
    }
    catch (ex) {
        return 'Done';
    }

    let msgCount = 0;
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessages);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesDelayed);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesNotVisible);

    if (msgCount === 0) {
        await SQS.deleteQueue({ QueueUrl: data.QueueUrl }).promise();

        let lambda = new AWS.Lambda({ apiVersion: '2015-03-31', region: 'us-west-2' });

        await lambda.deleteEventSourceMapping({ UUID: data.UUID }).promise();

        console.log(JSON.stringify(data));

        let ACQueueParams = {
            MessageBody: JSON.stringify({ expense: data.expense_id }),
            QueueUrl: "https://sqs.us-west-2.amazonaws.com/730404845529/qa_update_expense_queue	",
            DelaySeconds: 0,
        };

        await SQS.sendMessage(ACQueueParams).promise();
    }

    return 'Done';
};
