use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use hyper::{
  header,
  server::conn::AddrStream,
  service::{make_service_fn, service_fn},
  upgrade::{self, Upgraded},
  Body, Method, Request, Response, Server, StatusCode,
};
use log::{error, info};
use std::{convert::Infallible, future::Future, net::SocketAddr};
use tokio::{
  select,
  sync::mpsc,
  time::{Duration, Instant},
};
use tokio_tungstenite::WebSocketStream;
use tungstenite::{handshake, Message};

use crate::{shared::worker::AsyncHaltableJob, state::SliderState};

fn build_aprom_payload(payload: &[u8]) -> [u8; 97] {
  // UMIGURI LED payload layout (103 bytes):
  //   byte 0:   reserved
  //   bytes 1-48:  16 main cells  (3 bytes RGB each)
  //   bytes 49-93: 15 guide walls (3 bytes RGB each)
  //   bytes 94-102: 3 air strings (ignored)
  //
  // host-aprom payload layout (97 bytes):
  //   byte 0:       brightness (placeholder, overwritten by DivaSliderJob)
  //   bytes 1-93:   31 × BRG (interleaved cell/gw, reversed order)
  //   bytes 94-96:  zero padding (aBRG[31])
  //
  // Order matches the Python bridge:
  //   rgb_31 = [cell15, gw14, cell14, gw13, ..., cell1, gw0, cell0]

  let mut out = [0u8; 97];

  // Brightness placeholder — DivaSliderJob will overwrite from its config
  out[0] = 63;

  let mut out_idx = 1; // skip brightness byte

  // Interleave cells (reversed) and guide walls (reversed):
  //   cell15, gw14, cell14, gw13, ..., cell1, gw0, cell0
  for i in 0..15 {
    // Cell (15 - i): bytes 1 + (15 - i) * 3, for 3 bytes
    let cell_ofs = 1 + (15 - i) * 3;
    // Guide wall (14 - i): bytes 49 + (14 - i) * 3
    let wall_ofs = 49 + (14 - i) * 3;

    // Cell in BRG order (B = byte+2, R = byte+0, G = byte+1)
    out[out_idx] = payload[cell_ofs + 2];     // B
    out[out_idx + 1] = payload[cell_ofs];     // R
    out[out_idx + 2] = payload[cell_ofs + 1]; // G
    out_idx += 3;

    // Guide wall in BRG order
    out[out_idx] = payload[wall_ofs + 2];     // B
    out[out_idx + 1] = payload[wall_ofs];     // R
    out[out_idx + 2] = payload[wall_ofs + 1]; // G
    out_idx += 3;
  }

  // Last cell (cell0): bytes 1 + 0 * 3 = bytes 1..3
  out[out_idx] = payload[1 + 2];     // B
  out[out_idx + 1] = payload[1];     // R
  out[out_idx + 2] = payload[1 + 1]; // G
  // out_idx += 3; — not needed, remaining bytes are already zero

  out
}

async fn error_response() -> Result<Response<Body>, Infallible> {
  Ok(
    Response::builder()
      .status(StatusCode::NOT_FOUND)
      .body(Body::from(format!("Not found")))
      .unwrap(),
  )
}

async fn handle_umgr_leds(ws_stream: WebSocketStream<Upgraded>, state: SliderState, faster: bool) {
  let (mut ws_write, mut ws_read) = ws_stream.split();

  let (msg_write, mut msg_read) = mpsc::unbounded_channel::<Message>();

  let write_task = async move {
    loop {
      let message = msg_read.recv().await;
      match message {
        Some(msg) => match ws_write.send(msg).await.ok() {
          Some(_) => {}
          None => {
            break;
          }
        },
        None => {
          break;
        }
      }
    }
  };

  let msg_write_handle = msg_write.clone();
  let state_handle = state.clone();
  let read_task = async move {
    let mut latest_lights = Instant::now();
    let delay = match faster {
      true => Duration::from_micros(33333),
      false => Duration::from_micros(66666),
    };

    loop {
      match ws_read.next().await {
        Some(msg) => match msg {
          Ok(msg) => match msg {
            Message::Binary(msg) => {
              if msg.len() < 3 {
                error!("Unexpected length of UMGR led packet");
                break;
              }

              let version = msg[0];
              let opcode = msg[1];
              let payload_len = msg[2];
              let payload = &msg[3..];

              if payload_len as usize != payload.len() {
                error!("Unexpected length of UMGR led packet");
                break;
              }

              match (version, opcode, payload_len) {
                (0x01, 0x10, 103) => {
                  // SetLED — build the 97-byte host-aprom payload
                  let aprom_payload = build_aprom_payload(payload);

                  let mut lights_handle = state_handle.lights.lock();
                  lights_handle.host_aprom_payload = Some(aprom_payload);

                  if latest_lights.elapsed() > delay {
                    lights_handle.dirty = true;
                    latest_lights = Instant::now();
                  }
                }
                (0x01, 0x11, 0) => {
                  // Initialize
                  info!("UMGR LED Initialize");
                  msg_write_handle
                    .send(Message::Binary(vec![0x01, 0x19, 0x00]))
                    .ok();
                }
                (0x01, 0x12, 4) => {
                  // Client Ping
                  info!("UMGR LED Ping");
                  msg_write_handle
                    .send(Message::Binary(vec![
                      0x01, 0x1a, 6, payload[0], payload[1], payload[2], payload[3], 0x51, 0xed,
                    ]))
                    .ok();
                }
                (0x01, 0xD0, 0) => {
                  // Client RequestServerInfo
                  info!("UMGR LED Info");
                  let server_major: u16 = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
                  let server_minor: u16 = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap();
                  let response = [
                    // Command
                    vec![0x01, 0xd8, 44],
                    // Server name
                    b"slider-io\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                    // Server version
                    vec![
                      (server_major >> 8) as u8,
                      (server_major & 0xff) as u8,
                      (server_minor >> 8) as u8,
                      (server_minor & 0xff) as u8,
                    ],
                    // Reserved
                    vec![0x00, 0x00],
                    // Hardware name
                    b"tasoller-aprom\x00\x00".to_vec(),
                    // Hardware version
                    vec![0x00, 0x01, 0x00, 0x01],
                    // Reserved
                    vec![0x00, 0x00],
                  ]
                  .concat();

                  msg_write_handle.send(Message::Binary(response)).ok();
                }
                _ => {
                  break;
                }
              }
            }
            Message::Close(_) => {
              info!("Websocket connection closed");
              let mut lights_handle = state_handle.lights.lock();
              lights_handle.reset();
              break;
            }
            _ => {}
          },
          Err(e) => {
            error!("Websocket connection error: {}", e);
            let mut lights_handle = state_handle.lights.lock();
            lights_handle.reset();
            break;
          }
        },
        None => {
          break;
        }
      }
    }
  };

  select! {
    _ = read_task => {}
    _ = write_task => {}
  };
}

async fn handle_websocket(
  mut request: Request<Body>,
  state: SliderState,
  faster: bool,
) -> Result<Response<Body>, Infallible> {
  let res = match handshake::server::create_response_with_body(&request, || Body::empty()) {
    Ok(res) => {
      tokio::spawn(async move {
        match upgrade::on(&mut request).await {
          Ok(upgraded) => {
            let ws_stream = WebSocketStream::from_raw_socket(
              upgraded,
              tokio_tungstenite::tungstenite::protocol::Role::Server,
              None,
            )
            .await;

            handle_umgr_leds(ws_stream, state, faster).await;
          }

          Err(e) => {
            error!("Websocket upgrade error: {}", e);
          }
        }
      });

      res
    }
    Err(e) => {
      error!("Websocket creation error: {}", e);
      Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(format!("Failed to create websocket: {}", e)))
        .unwrap()
    }
  };
  Ok(res)
}

async fn handle_request(
  request: Request<Body>,
  remote_addr: SocketAddr,
  state: SliderState,
  faster: bool,
) -> Result<Response<Body>, Infallible> {
  let method = request.method();
  let path = request.uri().path();
  if method != Method::GET {
    error!(
      "Server unknown method {} -> {} {}",
      remote_addr, method, path
    );
    return error_response().await;
  }
  info!("Server {} -> {} {}", remote_addr, method, path);

  match (
    request.uri().path(),
    request.headers().contains_key(header::UPGRADE),
  ) {
    ("/", true) => handle_websocket(request, state, faster).await,
    _ => error_response().await,
  }
}

pub struct UmgrHostApromJob {
  state: SliderState,
  faster: bool,
  port: u16,
}

impl UmgrHostApromJob {
  pub fn new(state: &SliderState, faster: &bool, port: &u16) -> Self {
    Self {
      state: state.clone(),
      faster: *faster,
      port: *port,
    }
  }
}

#[async_trait]
impl AsyncHaltableJob for UmgrHostApromJob {
  async fn run<F: Future<Output = ()> + Send>(self, stop_signal: F) {
    let state = self.state.clone();
    let faster = self.faster;
    let make_svc = make_service_fn(|conn: &AddrStream| {
      let remote_addr = conn.remote_addr();
      let make_svc_state = state.clone();
      async move {
        Ok::<_, Infallible>(service_fn(move |request: Request<Body>| {
          let svc_state = make_svc_state.clone();
          handle_request(request, remote_addr, svc_state, faster)
        }))
      }
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
    info!("UMGR host-aprom LED websocket server listening on {}", addr);

    let server = Server::bind(&addr)
      .serve(make_svc)
      .with_graceful_shutdown(stop_signal);

    if let Err(e) = server.await {
      info!("UMGR host-aprom LED websocket server stopped: {}", e);
    }
  }
}
