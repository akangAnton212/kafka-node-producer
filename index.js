var app = require('express')();
var http = require('http').Server(app);
const kafka = require('kafka-node');
require('dotenv').config()


try {
    const Producer = kafka.Producer;
    const client = new kafka.KafkaClient(process.env.KAFKA_SERVER);
    const producer = new Producer(client);
    const kafka_topic = 'contoh-topic';
    console.log(kafka_topic);
    let payloads = [
      {
        topic: kafka_topic,
        messages: "Akang Anton awe awe cuyy"
      }
    ];
  
    producer.on('ready', async function() {
        for (let index = 0; index < 20; index++) {
            setTimeout(() => {
                let push_status = producer.send(payloads, (err, data) => {
                    if (err) {
                        console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
                    } else {
                        console.log('[kafka-producer -> '+kafka_topic+']: broker update success, ke- '+index);
                    }
                });
            }, 1500);
        }
    });
  
    producer.on('error', function(err) {
      console.log(err);
      console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
      throw err;
    });
  }
  catch(e) {
    console.log(e);
  }



http.listen(process.env.APP_LISTEN || 4700, () => {
    console.log('producer listen on : '  +process.env.APP_LISTEN);
});