import json
import os
import time

import pika


def connect_to_rabbitmq():
    rabbitmq_url = os.environ["RABBITMQ_URL"]
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(rabbitmq_url)
            )
            print("Worker: conectado a RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Worker: RabbitMQ todavía no está disponible; reintento en 5 s.")
            time.sleep(5)


def make_callback(event_name):
    def callback(channel, method, properties, body):
        try:
            task_data = json.loads(body)
            if not isinstance(task_data, dict) or not task_data.get("title"):
                raise ValueError("el mensaje no contiene un title válido")
        except (json.JSONDecodeError, ValueError) as error:
            print(f"Worker: mensaje inválido; se rechaza: {error}")
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )
            return

        if event_name == "task_created":
            print(
                " [x] Tarea recibida: "
                f"ID={task_data.get('id')}, Título='{task_data['title']}'"
            )
        else:
            print(
                " [+] Tarea completada: "
                f"ID={task_data.get('id')}, Título='{task_data['title']}'"
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    return callback


def main():
    connection = connect_to_rabbitmq()
    try:
        channel = connection.channel()
        channel.queue_declare(queue="task_created", durable=True)
        channel.queue_declare(queue="task_completed", durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue="task_created",
            on_message_callback=make_callback("task_created"),
        )
        channel.basic_consume(
            queue="task_completed",
            on_message_callback=make_callback("task_completed"),
        )

        print(" [*] Esperando mensajes. Para salir presione CTRL+C")
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Worker: interrumpido.")
