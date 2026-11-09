import json
import os

import pika
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(255), nullable=True)
    done = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default=db.false(),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "done": self.done,
        }


def publish_message(queue_name, message):
    rabbitmq_url = os.environ["RABBITMQ_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
    finally:
        if connection.is_open:
            connection.close()


def publish_task_event(queue_name, task):
    try:
        publish_message(queue_name, task.to_dict())
    except (pika.exceptions.AMQPError, OSError):
        app.logger.exception("No se pudo publicar el evento en %s", queue_name)
        return (
            jsonify(
                {
                    "error": (
                        "La tarea se guardó en PostgreSQL, pero no se pudo "
                        "publicar el mensaje en RabbitMQ."
                    ),
                    "task": task.to_dict(),
                }
            ),
            503,
        )
    return None


@app.get("/tasks")
def get_tasks():
    tasks = db.session.execute(
        db.select(Task).order_by(Task.id)
    ).scalars()
    return jsonify({"tasks": [task.to_dict() for task in tasks]})


@app.get("/tasks/<int:task_id>")
def get_task(task_id):
    task = db.session.get(Task, task_id)
    if task is None:
        return jsonify({"error": "Task not found"}), 404
    return jsonify({"task": task.to_dict()})


@app.post("/tasks")
def create_task():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "A JSON object is required"}), 400

    title = payload.get("title")
    if not isinstance(title, str) or not title.strip():
        return jsonify({"error": "A non-empty title is required"}), 400
    if len(title.strip()) > 120:
        return jsonify({"error": "title must be at most 120 characters"}), 400

    description = payload.get("description", "")
    if not isinstance(description, str):
        return jsonify({"error": "description must be a string"}), 400
    if len(description) > 255:
        return jsonify({"error": "description must be at most 255 characters"}), 400

    task = Task(title=title.strip(), description=description, done=False)
    db.session.add(task)
    db.session.commit()

    publish_error = publish_task_event("task_created", task)
    if publish_error is not None:
        return publish_error
    return jsonify({"task": task.to_dict()}), 201


@app.put("/tasks/<int:task_id>/complete")
def complete_task(task_id):
    task = db.session.get(Task, task_id)
    if task is None:
        return jsonify({"error": "Task not found"}), 404

    task.done = True
    db.session.commit()

    publish_error = publish_task_event("task_completed", task)
    if publish_error is not None:
        return publish_error
    return jsonify({"task": task.to_dict()})


@app.delete("/tasks/<int:task_id>")
def delete_task(task_id):
    task = db.session.get(Task, task_id)
    if task is None:
        return jsonify({"error": "Task not found"}), 404

    db.session.delete(task)
    db.session.commit()
    return "", 204


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(host="0.0.0.0", port=5000, debug=False)
