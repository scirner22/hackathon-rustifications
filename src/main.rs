extern crate actix;
extern crate actix_web;
extern crate futures;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::collections::{ HashMap, HashSet };
use std::time::{ Duration, Instant };

use actix::prelude::*;
use actix_web::{
    http, server, ws, App, AsyncResponder, Error, HttpRequest, HttpMessage, HttpResponse,
};
use futures::Future;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

struct AppState {
    accepted_types: HashSet<String>,
    arbiter_addr: Addr<WsArbiter>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    #[serde(rename = "type")]
    type_alias: String,
    version: u32,
    payload: serde_json::Value,
    created_at: String,
    created_at_micros: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Identifier {
    organization_id: u32,
    user_id: u32,
}

struct WebSocket {
    heart_beat: Instant,
    id: Identifier,
}

impl WebSocket {
    fn new(id: Identifier) -> Self {
        WebSocket {
            heart_beat: Instant::now(),
            id,
        }
    }

    fn pulse(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.heart_beat) > CLIENT_TIMEOUT {
                ctx.stop();
            } else {
                ctx.ping("");
            }
        });
    }
}

impl Actor for WebSocket {
    type Context = ws::WebsocketContext<Self, AppState>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.pulse(ctx);

        let addr = ctx.address();
        ctx.state()
            .arbiter_addr
            .send(Connect {
                addr: addr.recipient(),
                id: self.id.clone(),
            })
            .into_actor(self)
            .then(move |res, _act, ctx| {
                match res {
                    Ok(_) => (),
                    _ => ctx.stop(),
                }
                actix::fut::ok(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        let id = self.id.clone();
        ctx.state().arbiter_addr.do_send(Disconnect { id });
        Running::Stop

    }
}

impl Handler<Message> for WebSocket {
    type Result = ();

    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for WebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match msg {
            ws::Message::Ping(msg) => {
                self.heart_beat = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.heart_beat = Instant::now();
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            _ => {
                // this websocket is a one way connection, ignore text and binary receives
            }
        }
    }
}

#[derive(Message)]
struct Message(String);

#[derive(Message)]
struct Connect {
    addr: Recipient<Message>,
    id: Identifier,
}

#[derive(Message)]
struct Disconnect {
    id: Identifier,
}

#[derive(Message)]
struct FwdMessage {
    id: Identifier,
    event: Event,
}

struct WsArbiter {
    handles: HashMap<Identifier, Recipient<Message>>,
}

impl Default for WsArbiter {
    fn default() -> Self {
        WsArbiter { handles: HashMap::new() }
    }
}

impl Actor for WsArbiter {
    type Context = Context<Self>;
}

impl Handler<Connect> for WsArbiter {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        println!("Adding {:?}", msg.id);
        self.handles.entry(msg.id.clone()).or_insert(msg.addr);
    }
}

impl Handler<Disconnect> for WsArbiter {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) -> Self::Result {
        println!("Removing {:?}", msg.id);
        self.handles.remove(&msg.id);
    }
}

impl Handler<FwdMessage> for WsArbiter {
    type Result = ();

    fn handle(&mut self, msg: FwdMessage, _: &mut Context<Self>) -> Self::Result {
        println!("Fwding {:?}", msg.id);
        if let Some(addr) = self.handles.get(&msg.id) {
            println!("user logged in!");
            let msg_string = serde_json::to_string(&msg.event).unwrap();
            addr.do_send(Message(msg_string)).unwrap();
        } else {
            println!("user not logged in!");
        }
    }
}

fn ingest(req: HttpRequest<AppState>) -> impl Future<Item=HttpResponse, Error=Error> {
    req.json()
        .from_err()
        .and_then(move |body: Event| {
            let accepted_types = &req.state().accepted_types;

            if accepted_types.contains(&body.type_alias) {
                println!("accepted type");
                req.state()
                    .arbiter_addr
                    .do_send(FwdMessage {
                        id: Identifier { organization_id: 1, user_id: 1},
                        event: body,
                    })
            }

            Ok(HttpResponse::Created().body(""))
        })
        .responder()
}

fn ws_connect(req: &HttpRequest<AppState>) -> Result<HttpResponse, Error> {
    //let organization_id: u8 = req.headers().get("XBeamOrganization").unwrap().into();
    let organization_id = 1;
    let user_id = 1;
    let id = Identifier { organization_id, user_id };

    ws::start(req, WebSocket::new(id))
}

fn main() {
    let system = actix::System::new("hackathon");

    let arbiter = Arbiter::start(|_| WsArbiter::default());

    server::new(move || {
        let mut accepted_types = HashSet::new();
        accepted_types.insert(String::from("overlap"));
        let state = AppState {
            accepted_types,
            arbiter_addr: arbiter.clone(),
        };

        App::with_state(state)
            .resource("/ingest", |r| r.method(http::Method::POST).with_async(ingest))
            .resource("/connect", |r| r.method(http::Method::GET).f(ws_connect))
    })
    .bind("0.0.0.0:80")
    .unwrap()
    .start();

    println!("Server running at 0.0.0.0:80");
    let _ = system.run();
}
