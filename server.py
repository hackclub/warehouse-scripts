from http.server import HTTPServer, BaseHTTPRequestHandler

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        message = "This is just a blank page for hackclub/warehouse-scripts. Please set up the scripts as scheduled tasks in Coolify!"
        self.wfile.write(message.encode())

if __name__ == "__main__":
    server = HTTPServer(('0.0.0.0', 3000), SimpleHandler)
    print("Server started on port 3000")
    server.serve_forever() 