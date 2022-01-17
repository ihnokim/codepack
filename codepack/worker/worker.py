from codepack import Code
from kafka import KafkaConsumer
import json


def consumer_example():
    consumer = KafkaConsumer('test', bootstrap_servers='1.2.3.4:9092',
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             consumer_timeout_ms=1000,
                             group_id='group1')
    while True:
        try:
            buffer = consumer.poll(timeout_ms=1000)
            for tp, msgs in buffer.items():
                print('topic: %s, partition: %d' % (tp.topic, tp.partition))
                for msg in msgs:
                    try:
                        print('offset: %d, key: %s, value: %s' % (msg.offset, msg.key, msg.value))
                        content = json.loads(msg.value.decode('utf-8'))
                        print(content)
                        code = Code(id=content['id'], serial_number=content['_id'], dependency=content['dependency'])
                        ret = code(*content['args'], **content['kwargs'])
                        print(ret)
                    except Exception as e:
                        print(e)
                        continue
        except KeyboardInterrupt:
            print("exit")
            break
