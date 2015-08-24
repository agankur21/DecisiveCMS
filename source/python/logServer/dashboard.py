import web
import os
from jinja2 import Environment,FileSystemLoader
from kafka import SimpleProducer, KafkaClient



urls = (
    '/evtcap', 'EventCapture',
    '/', 'index'


)

app = web.application(urls, globals())


kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(
        loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
        extensions=extensions,
    )
    jinja_env.globals.update(globals)

    return jinja_env.get_template(template_name).render(context)


class EventCapture:
    def POST(self):
        value = web.data()
        producer.send_messages('logs',str(value) )


class index:
    def GET(self):
        return render_template('index.html')


if __name__ == "__main__":
    app.run()
