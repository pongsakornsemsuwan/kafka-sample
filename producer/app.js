const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();

app.use(express.json());

app.post('/user', async (req, res, next) => {
  const kafka = new Kafka({
    "clientId": "myapp",
    "brokers" :["pongsakorn.local:9092"]
  })
  const producer = kafka.producer();
  console.log("Connecting.....")
  await producer.connect()
  console.log("Connected!")
  //A-M 0 , N-Z 1
  const partition = req.body.name[0] < "N" ? 0 : 1;
  const result =  await producer.send({
      "topic": "Users",
      "messages": [
          {
              "value": JSON.stringify(req.body),
              "partition": partition
          }
      ]
  })

  console.log(`Send Successfully! ${JSON.stringify(result)}`)
  await producer.disconnect();
  res.send('User created');
});

app.listen(3000, () => {
  console.log('Listening @ 3000');
});
