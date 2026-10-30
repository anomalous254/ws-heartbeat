# ws-heartbeat

Keep track of which WebSocket clients are still alive, and drop the ones that went quiet.

Most servers start one timer for every connection. With thousands of users, that's thousands of timers. This crate uses **one** background task that checks everybody every few seconds. It pings each client, and if a client hasn't answered in time, it closes that connection.

It works with any WebSocket framework. Actix and Axum are supported out of the box, and you can add your own in a few lines.

## How it works, in simple words

Think of a security guard walking through a building every few seconds.

1. The guard knocks on every door (sends a ping).
2. If someone answers, the guard notes the time.
3. If a door stays silent for too long, the guard locks it (closes the connection).

You only have to do three things:

1. Tell the registry when someone connects.
2. Tell it when a client shows a sign of life.
3. Tell it when someone leaves.

## Install

For Actix:

```bash
cargo add ws-heartbeat --features actix
```

For Axum:

```bash
cargo add ws-heartbeat --features axum
```

Or write it yourself in `Cargo.toml`:

```toml
[dependencies]
ws-heartbeat = { version = "0.1", features = ["actix"] }
```

You only download the framework you actually use.

## The basics

```rust
use std::time::Duration;
use ws_heartbeat::prelude::*;

// 1. Create the registry once. K is your id type, S is the session type.
let registry: HeartbeatRegistry<uuid::Uuid, ActixSession> = HeartbeatRegistry::new();

// 2. Start the guard once, at startup.
//    Check every 5 seconds, close anyone silent for 15 seconds.
registry.spawn_sweeper(Duration::from_secs(5), Duration::from_secs(15));

// 3. When a client connects:
registry.register(id, ActixSession(session.clone()));

// 4. When a pong (or any message) arrives:
registry.touch(&id);

// 5. When the connection ends:
registry.remove(&id);
```

`spawn_sweeper` must be called from inside a Tokio runtime. Actix and Axum both run on Tokio, so this just works.

## Actix example

```rust
use actix_web::{web, HttpRequest, HttpResponse, Error};
use actix_ws::Message;
use uuid::Uuid;
use ws_heartbeat::prelude::*;

type Registry = HeartbeatRegistry<Uuid, ActixSession>;

async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    registry: web::Data<Registry>,
) -> Result<HttpResponse, Error> {
    let (response, session, mut stream) = actix_ws::handle(&req, body)?;

    let id = Uuid::new_v4();
    registry.register(id, ActixSession(session.clone()));

    let registry = registry.clone();
    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = stream.recv().await {
            match msg {
                Message::Close(_) => break,
                // Pong, text, binary, anything counts as a sign of life
                _ => registry.touch(&id),
            }
        }
        registry.remove(&id);
    });

    Ok(response)
}
```

Start the sweeper once in `main`, before the server runs:

```rust
let registry = web::Data::new(Registry::new());
registry.spawn_sweeper(Duration::from_secs(5), Duration::from_secs(15));
```

## Axum example

Axum's socket can't be cloned, so each connection gets a small channel. A writer task reads from the channel and sends to the socket. The registry holds the sending end of that channel.

```rust
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use uuid::Uuid;
use ws_heartbeat::prelude::*;

type Registry = HeartbeatRegistry<Uuid, AxumSession>;

async fn handle_socket(socket: WebSocket, registry: Registry) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    let id = Uuid::new_v4();
    registry.register(id, AxumSession(tx));

    // Writer task: forwards channel messages (including pings) to the socket
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let is_close = matches!(msg, Message::Close(_));
            if sender.send(msg).await.is_err() || is_close {
                break;
            }
        }
    });

    // Reader loop: any incoming message means the client is alive
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Close(_) => break,
            _ => registry.touch(&id),
        }
    }

    registry.remove(&id);
    writer.abort();
}
```

## Using another framework

The registry doesn't know about any framework. It only knows one small promise, the `HeartbeatSession` trait:

```rust
use async_trait::async_trait;
use ws_heartbeat::HeartbeatSession;

#[derive(Clone)]
struct MySession { /* whatever your framework gives you */ }

#[async_trait]
impl HeartbeatSession for MySession {
    async fn send_ping(self) {
        // send a ping to this client
    }

    async fn close_session(self) {
        // close this connection
    }
}
```

Once that is written, `HeartbeatRegistry<YourId, MySession>` works the same way as in the examples above. Your session type needs to be `Clone`, `Send`, `Sync` and `'static`.

## Features

| Feature | What you get |
|---|---|
| `actix` | `ActixSession`, a wrapper around `actix_ws::Session` |
| `axum` | `AxumSession`, a wrapper around a channel sender for Axum sockets |

With no features turned on, you get just the registry and the trait.

## Good to know

Pick a timeout that is at least two or three times longer than the sweep interval. A 5 second sweep with a 15 second timeout gives each client about three chances to answer.

Always call `remove` when a connection ends, no matter the reason. Otherwise the registry keeps a dead entry until the sweeper cleans it up.

Browsers answer pings with pongs automatically, so you usually don't need any code on the client side.

## Minimum Rust version

Rust 1.85 or newer.

## License

Licensed under the Apache License, Version 2.0.