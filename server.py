#!/usr/bin/env python3
import http.server
import socketserver

class SimpleHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"hey, this is just a docker environment for tasks run from hackclub/warehouse-scripts")

if __name__ == "__main__":
    with socketserver.TCPServer(("", 3000), SimpleHandler) as httpd:
        print("Server running at http://0.0.0.0:3000")
        httpd.serve_forever() 