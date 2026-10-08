use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Wait time after a webhook call to allow async processing.
pub const WEBHOOK_PROCESSING_WAIT_MS: u64 = 300;

/// Wait time after a batch update to allow DB persistence.
pub const BATCH_UPDATE_WAIT_MS: u64 = 250;

/// Generic mock TCP server that responds with a fixed HTTP response
/// and optionally counts incoming requests.
fn spawn_mock_tcp_server(
    response: &'static str,
    hits: Option<Arc<AtomicUsize>>,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let hits = hits.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            if let Some(hits) = hits {
                                hits.fetch_add(1, Ordering::SeqCst);
                            }
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), shutdown_tx)
}

/// Spawn a mock webhook server that returns 200 OK and counts incoming requests.
pub fn spawn_webhook_server(hits: Arc<AtomicUsize>) -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_mock_tcp_server("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", Some(hits))
}

/// Spawn a mock webhook server that always returns 500 Internal Server Error.
pub fn spawn_500_webhook_server() -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_mock_tcp_server(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
        None,
    )
}

/// Spawn a mock webhook server that returns 500 for the first `fail_count` requests
/// and 200 OK thereafter. `hits` counts every incoming request. Useful for testing
/// outbox retry-then-succeed behavior.
pub fn spawn_flaky_webhook_server(
    hits: Arc<AtomicUsize>,
    fail_count: usize,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let hits = hits.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            // 0-indexed request number BEFORE this increment.
                            let n = hits.fetch_add(1, Ordering::SeqCst);
                            let response = if n < fail_count {
                                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
                            } else {
                                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                            };
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), shutdown_tx)
}

/// Spawn a mock webhook server that always returns 200 OK after a fixed delay,
/// counting incoming requests. Used to assert that the API path stays fast even
/// when the downstream consumer is slow.
pub fn spawn_slow_200_webhook_server(
    hits: Arc<AtomicUsize>,
    delay: std::time::Duration,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let hits = hits.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            hits.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(delay).await;
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), shutdown_tx)
}

/// Spawn a mock webhook server that always returns 302 Found with a redirect Location.
pub fn spawn_302_redirect_server() -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_mock_tcp_server(
        "HTTP/1.1 302 Found\r\nLocation: http://evil.com/steal-data\r\nContent-Length: 0\r\n\r\n",
        None,
    )
}

/// Captured HTTP request from the request capture server.
#[derive(Debug)]
pub struct CapturedRequest {
    pub request_line: String,
    pub headers: HashMap<String, String>,
    pub body: String,
}

/// Spawn a mock server that captures the full HTTP request (request line, headers, body)
/// and sends it via a oneshot channel. Returns 200 OK.
pub fn spawn_request_capture_server() -> (
    String,
    tokio::sync::oneshot::Receiver<CapturedRequest>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (capture_tx, capture_rx) = tokio::sync::oneshot::channel::<CapturedRequest>();
    let capture_tx = Arc::new(tokio::sync::Mutex::new(Some(capture_tx)));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let capture_tx = capture_tx.clone();
                        tokio::spawn(async move {
                            let mut buf = Vec::with_capacity(8192);
                            let mut tmp = [0u8; 4096];
                            let header_end;
                            loop {
                                let n = stream.read(&mut tmp).await.unwrap_or(0);
                                if n == 0 { return; }
                                buf.extend_from_slice(&tmp[..n]);
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    header_end = pos;
                                    break;
                                }
                            }
                            let header_section = String::from_utf8_lossy(&buf[..header_end]).to_string();
                            let body_start = header_end + 4;

                            let mut lines = header_section.lines();
                            let request_line = lines.next().unwrap_or("").to_string();
                            let mut headers = HashMap::new();
                            for line in lines {
                                if let Some((k, v)) = line.split_once(':') {
                                    headers.insert(k.trim().to_lowercase(), v.trim().to_string());
                                }
                            }

                            let content_length: usize = headers
                                .get("content-length")
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(0);

                            let already_read = buf.len() - body_start;
                            if already_read < content_length {
                                let remaining = content_length - already_read;
                                let mut body_buf = vec![0u8; remaining];
                                stream.read_exact(&mut body_buf).await.expect("failed to read request body");
                                buf.extend_from_slice(&body_buf);
                            }

                            let body = String::from_utf8_lossy(&buf[body_start..body_start + content_length]).to_string();

                            let captured = CapturedRequest {
                                request_line,
                                headers,
                                body,
                            };

                            if let Some(tx) = capture_tx.lock().await.take() {
                                let _ = tx.send(captured);
                            }

                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), capture_rx, shutdown_tx)
}

/// Spawn a mock server that captures HTTP headers and returns 200 OK.
pub fn spawn_header_capture_server() -> (
    String,
    tokio::sync::oneshot::Receiver<HashMap<String, String>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (headers_tx, headers_rx) = tokio::sync::oneshot::channel::<HashMap<String, String>>();
    let headers_tx = Arc::new(tokio::sync::Mutex::new(Some(headers_tx)));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let headers_tx = headers_tx.clone();
                        tokio::spawn(async move {
                            let mut data = Vec::new();
                            let mut buf = [0u8; 4096];
                            loop {
                                let read = stream.read(&mut buf).await.unwrap_or_default();
                                if read == 0 {
                                    break;
                                }
                                data.extend_from_slice(&buf[..read]);
                                if data.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }

                            let mut headers = HashMap::new();
                            let text = String::from_utf8_lossy(&data);
                            for line in text.lines().skip(1) {
                                let line = line.trim_end();
                                if line.is_empty() {
                                    break;
                                }
                                if let Some((k, v)) = line.split_once(':') {
                                    headers.insert(
                                        k.trim().to_ascii_lowercase(),
                                        v.trim().to_string(),
                                    );
                                }
                            }

                            if let Some(tx) = headers_tx.lock().await.take() {
                                let _ = tx.send(headers);
                            }

                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), headers_rx, shutdown_tx)
}
