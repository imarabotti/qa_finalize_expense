const AWS = require('aws-sdk');

const SQS = new AWS.SQS({ apiVersion: '2012-11-05', region: 'us-west-2' });
const S3 = new AWS.S3({ apiVersion: '2006-03-01', region: 'us-west-2' });
const lambda = new AWS.Lambda({ apiVersion: '2015-03-31', region: 'us-west-2' });

exports.handler = async(event) => {
    const message = JSON.parse(event.Records[0].body);

    let s3Params = { Bucket: message.bucket, Key: message.ruta };
    let file;
    let data;
    let response;

    //* Descargo el archivo
    try {
        file = await S3.getObject(s3Params).promise();
    } catch (ex) {
        //* Si no existe archivo, no hay datos ni de queue ni event source mapping
        return console.log(`El archivo ${message.ruta} no existe. Done!`);
    }

    //* Parseo el archivo
    try {
        data = JSON.parse(file.Body.toString());
    } catch (ex) {
        //* Si no puedo leer el archivo no puedo continuar
        return console.log(`No pude parsear el archivo ${message.ruta}`);
    }

    //* Consulto los mensajes de la queue
    try {
        let attrParams = {
            QueueUrl: data.QueueUrl,
            AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed', 'ApproximateNumberOfMessagesNotVisible']
        };

        response = await SQS.getQueueAttributes(attrParams).promise();
    } catch (ex) {
        this.finalizeExpense(data, s3Params);
        return console.log(`La queue ${data.QueueUrl} no existe`);
    }
        
    let msgCount = 0;
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessages);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesDelayed);
    msgCount += parseInt(response.Attributes.ApproximateNumberOfMessagesNotVisible);

    if (msgCount === 0) {
        try {
            await SQS.deleteQueue({ QueueUrl: data.QueueUrl }).promise();
        } catch (ex) {
            console.log(`La queue ${data.QueueUrl} no existe`);
        }

        this.finalizeExpense(data, s3Params);

        return console.log(`Expensa ${data.expense_id} terminada, actualiza DB en update_expense_queue`);
    } else {
        await this.resetLambda(event);

        return console.log('Reseteando lambda expensa ${data.expense_id}');
    }
};

exports.resetLambda = async (event) => {
    // No termino, porque la cantidad de msgs no es 0 (osea todavia hay trabajos por hacer)
    // Entonces reenvio el mensaje con delay

    let ACQueueParams = {
        MessageBody: event.Records[0].body,
        QueueUrl: "https://sqs.us-west-2.amazonaws.com/730404845529/prod_finalize_expense_queue",
        DelaySeconds: 10,
    };

    await SQS.sendMessage(ACQueueParams).promise();

    return;
};

exports.finalizeExpense = async(data, s3Params) => {
    try {
        await lambda.deleteEventSourceMapping({ UUID: data.UUID }).promise();
    } catch (ex) {
        if (ex.code === "ResourceInUseException") {
            console.log(`Reseteando lambda por eventSource ${data.UUID} en uso. Expensa ${data.expense_id}`);
        } else if (ex.code === "ResourceNotFoundException") {
            console.log(`El eventSource ${data.UUID} ya esta eliminado. Expensa ${data.expense_id}`);
        } else {
            console.log(ex);
        }
    }

    let ACQueueParams = {
        MessageBody: JSON.stringify({ expense: data.expense_id }),
        QueueUrl: "https://sqs.us-west-2.amazonaws.com/730404845529/prod_update_expense_queue",
        DelaySeconds: 0,
    };

    await SQS.sendMessage(ACQueueParams).promise();

    await S3.deleteObject(s3Params).promise();
}