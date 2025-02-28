import os
from flask import Flask, request, jsonify
import multiprocessing
from twitch_gen import run_twitch
from nexmark_gen import run_nexmark
import logging
import sys

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

app = Flask(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')
    
event = multiprocessing.Event()
    
@app.route('/start', methods=['POST'])
def start():
    """
    REST API 路由，用于触发 start_produce 函数。
    """
    global event
    event.set()
    return jsonify({"message": "Production started"}), 200


if __name__ == '__main__':
    topic = os.getenv('TOPIC')
    if topic == 'twitch':
        gen_process = multiprocessing.Process(target=run_twitch, args=(event,))
    elif topic == 'q7' or topic == 'q8':
        gen_process = multiprocessing.Process(target=run_nexmark, args=(event,))
    else:
        raise ValueError("Invalid topic:", topic)

    gen_process.start()

    app.run(host='0.0.0.0', port=35000)
        
