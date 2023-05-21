import fetch from "node-fetch";
import cors from "cors";
import express from "express";
import path from "path";
import { Kafka } from "kafkajs";

const PORT = 3000;

const kafka = new Kafka({
  clientId: "kafka",
  brokers: ["localhost:9092"],
});

//КАМПУС
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "my-group" });

const app = express();
app.use(cors());
app.use(express.static(path.join("client", "src")));

//Запись данных в топик YarmarkaTopic
async function sendMessages(key, obj_value) {
  await producer.connect();
  await producer.send({
    topic: "YarmarkaTopic",
    messages: [
      {
        key: key,
        value: obj_value,
      },
    ],
  });
  await producer.disconnect();
}

//Чтение сообщений из топика CampusTopic
async function consumeMessages() {
  const messages = [];
 try {
  await consumer.connect();
  //await consumer.pause({ topic: "YarmarkaCampus", partitions: ["0"] });
  await consumer.subscribe({ topic: "CampusTopic", fromBeginning: false});
  await new Promise(async (resolve) => {
    let timeout = setTimeout(resolve, 1000);
    consumer.run({
      eachMessage: ({ topic, partition, message }) => {
        clearTimeout(timeout);
        timeout = setTimeout(resolve, 200);
        messages.push(message.value.toString());
      },
    });
    //consumer.seek({ topic: "YarmarkaCampus", partition: 1});
  });
  //await consumer.resume({ topic: "YarmarkaCampus", partitions: ["0"] });
  await consumer.disconnect();
}
catch {
}
  return messages;
}

//КАМПУС
app.get("/api/messages", async function (req, res) {
  console.log("КАМПУС");
  try {
    if (req.query.obj) {
      var message = "";
      if (req.query.message) {
        message = req.query.message;
      }
      console.log("message: ", message);
      switch (req.query.obj) {
        case "send1":
          console.log("Отправка");
          await sendMessages("0", message);
          break;
        case "get1":
          console.log("Получка");
          const getMessages = await consumeMessages();
          console.log("messages from YP: " + getMessages);
          res.send(getMessages);
          break;
        default:
          console.log("Other:", messages);
      }     
    }
  } catch (error) {
    res.send(error).status(400);
  }
});

app.listen(PORT, () => {
  console.log(`server started http://localhost:${PORT}`);
});
