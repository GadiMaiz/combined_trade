from flask import Flask, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app)


@socketio.on('message')
def handle_message(message):
    print('received message: ' + message)


def some_function():
    socketio.emit('some event', {'data': 42})


@socketio.on('connect')
def test_connect():
    socketio.emit('my response', {'data': 'Connected'})


@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    socketio.run(app, port=5001)
