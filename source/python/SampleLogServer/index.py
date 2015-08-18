import web
import os

urls = (
    '/logs', 'logs',
    '/index', 'index'

)

app = web.application(urls, globals())

class index:
    def GET(self):
        return 'Hello, world from web.py!'

class logs:
    def POST(self):
        data = web.data()
        if os.path.exists("log-file.txt"):
            with open("log-file.txt", "a") as myfile:
                myfile.write(str(data)+'\n')
        else:
            with open('log-file.txt', 'w') as f:
                f.write(str(data)+'\n')


if __name__ == "__main__":
   app.run()
