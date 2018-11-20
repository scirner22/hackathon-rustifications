extern crate actix;
extern crate actix_web;
extern crate futures;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::collections::{ HashMap, HashSet };
use std::sync::{ Arc, Mutex };
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
    ws_handles: Arc<Mutex<HashMap<Identifier, Addr<WebSocket>>>>,
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

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
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
                println!("heartbeat failed - disconnecting!");
                ctx.stop();
            } else {
                println!("Sending ping!");
                ctx.ping("");
            }
        });
    }
}

impl Actor for WebSocket {
    type Context = ws::WebsocketContext<Self, AppState>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Starting actor!");
        self.pulse(ctx);

        let handles_mutex = ctx.state().ws_handles.clone();
        let mut handles = handles_mutex.lock().unwrap();
        handles.entry(self.id.clone()).or_insert(ctx.address());
        println!("size: {:?}", handles.len());
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        println!("Stopped actor!");

        let handles_mutex = ctx.state().ws_handles.clone();
        let mut handles = handles_mutex.lock().unwrap();
        handles.remove(&self.id);
        println!("size: {:?}", handles.len());
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for WebSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        println!("WS: {:?}", msg);
        {
            let handles_mutex = ctx.state().ws_handles.clone();
            let handles = handles_mutex.lock().unwrap();
            println!("size: {:?}", handles.len());
            println!("handles: {:?}", handles.keys().collect::<Vec<_>>());
        }
        match msg {
            ws::Message::Ping(msg) => {
                println!("Got ping!");
                self.heart_beat = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                println!("Got pong!");
                self.heart_beat = Instant::now();
            }
            ws::Message::Close(_) => {
                println!("In close!");
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
        msg.id;
    }
}

impl Handler<Disconnect> for WsArbiter {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) -> Self::Result {
        println!("Adding {:?}", msg.id);
        self.handles.remove(&msg.id);
        msg.id;
    }
}

fn ingest(req: HttpRequest<AppState>) -> impl Future<Item=HttpResponse, Error=Error> {
    req.json()
        .from_err()
        .and_then(move |body: Event| {
            let accepted_types = &req.state().accepted_types;

            if accepted_types.contains(&body.type_alias) {
                println!("state accepted types contains what we matching on");
                {
                    let handles_mutex = req.state().ws_handles.clone();
                    let handles = handles_mutex.lock().unwrap();
                    println!("size: {:?}", handles.len());
                    println!("handles: {:?}", handles.keys().collect::<Vec<_>>());
                }
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
    {
        let handles_mutex = req.state().ws_handles.clone();
        let handles = handles_mutex.lock().unwrap();
        println!("size: {:?}", handles.len());
        println!("handles: {:?}", handles.keys().collect::<Vec<_>>());
    }

    let actor = WebSocket::new(id);
    // let addr = WebSocket::create(|ctx| {
    //     WebSocket::new(id)
    // });
    ws::start(req, actor)
}

fn main() {
    let system = actix::System::new("hackathon");

    let arbiter = Arbiter::start(|_| WsArbiter::default());

    server::new(|| {
        let mut accepted_types = HashSet::new();
        accepted_types.insert(String::from("overlap"));
        let state = AppState {
            accepted_types,
            ws_handles: Arc::new(Mutex::new(HashMap::new()))
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
