const AWS = require('aws-sdk');

const SQS = new AWS.SQS({ apiVersion: '2012-11-05', region: 'us-west-2' });
const S3 = new AWS.S3({ apiVersion: '2006-03-01', region: 'us-west-2' });
const lambda = new AWS.Lambda({ apiVersion: '2015-03-31', region: 'us-west-2' });

exports.handler = async(event) => {
    const message = JSON.parse(event.Records[0].body);

    let s3Params = { Bucket: message.bucket, Key: message.ruta };
    let file;

    try {
        file = await S3.getObject(s3Params).promise();
    } catch (ex) {
        return console.log('El archivo no existe. Done!');
    }

    let data = JSON.parse(file.Body.toString());
    let response;

    try {
        let attrParams = {
            QueueUrl: data.QueueUrl,
            AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed', 'ApproximateNumberOfMessagesNotVisible']
        };

        response = await SQS.getQueueAttributes(attrParams).promise();
    } catch (ex) {
        return console.log('La Queue no existe');
    }

    let msgCount = 0;
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessages);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesDelayed);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesNotVisible);

    if (msgCount === 0) {
        await SQS.deleteQueue({ QueueUrl: data.QueueUrl }).promise();

        await lambda.deleteEventSourceMapping({ UUID: data.UUID }).promise();

        console.log(JSON.stringify(data));

        let ACQueueParams = {
            MessageBody: JSON.stringify({ expense: data.expense_id }),
            QueueUrl: "https://sqs.us-west-2.amazonaws.com/730404845529/qa_update_expense_queue",
            DelaySeconds: 0,
        };

        await SQS.sendMessage(ACQueueParams).promise();

        await S3.deleteObject(s3Params).promise();

        return console.log('Expensa terminada, actualiza DB en update_expense_queue');
    } else {
        // No termino, porque la cantidad de msgs no es 0 (osea todavia hay trabajos por hacer)
        // Entonces reenvio el mensaje con delay

        let ACQueueParams = {
            MessageBody: event.Records[0].body,
            QueueUrl: "https://sqs.us-west-2.amazonaws.com/730404845529/qa_finalize_expense_queue",
            DelaySeconds: 10,
        };

        await SQS.sendMessage(ACQueueParams).promise();

        return console.log('Reseteo el lambda');
    }
};
