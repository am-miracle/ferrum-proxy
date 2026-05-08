from http.server import BaseHTTPRequestHandler, HTTPServer
import sys


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok\n")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        body = f"backend port={self.server.server_port} path={self.path}\n"
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass


def main():
    if len(sys.argv) != 2:
        print("usage: python3 scripts/test_backend.py <port>")
        raise SystemExit(1)

    port = int(sys.argv[1])
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"listening on http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
