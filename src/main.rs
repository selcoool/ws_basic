mod db;

use actix_web::{
    rt,
    web,
    App,
    Error,
    HttpRequest,
    HttpResponse,
    HttpServer,
};

use actix_ws;

use futures_util::StreamExt;

use serde::Deserialize;

use sqlx::MySqlPool;

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use tokio::sync::mpsc;





// ================= STATE =================

struct AppState {
    users: Mutex<HashMap<i32, mpsc::UnboundedSender<String>>>,
    groups: Mutex<HashMap<i32, HashSet<i32>>>,
}





// ================= REQUEST =================

#[derive(Deserialize)]
struct CreateMessage {
    sender_id: i32,
    receiver_id: Option<i32>,
    group_id: Option<i32>,
    message_type: String,
    content: String,
}





// ================= WS =================

async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    path: web::Path<i32>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {

    let user_id = path.into_inner();

    let (res, mut session, mut msg_stream) =
        actix_ws::handle(&req, stream)?;

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    data.users.lock().unwrap().insert(user_id, tx);

    let state = data.clone();





    rt::spawn(async move {

        loop {
            tokio::select! {

                Some(msg) = rx.recv() => {
                    let _ = session.text(msg).await;
                }

                Some(Ok(msg)) = msg_stream.next() => {
                    if let actix_ws::Message::Close(_) = msg {
                        break;
                    }
                }

                else => break
            }
        }

        state.users.lock().unwrap().remove(&user_id);
    });

    Ok(res)
}





// ================= CREATE MESSAGE =================

async fn create_message(
    data: web::Data<AppState>,
    pool: web::Data<MySqlPool>,
    body: web::Json<CreateMessage>,
) -> HttpResponse {

    let result = sqlx::query(
        r#"
        INSERT INTO messages
        (sender_id, receiver_id, group_id, message_type, content)
        VALUES (?, ?, ?, ?, ?)
        "#
    )
    .bind(body.sender_id)
    .bind(body.receiver_id)
    .bind(body.group_id)
    .bind(&body.message_type)
    .bind(&body.content)
    .execute(pool.get_ref())
    .await
    .unwrap();

    let id = result.last_insert_id() as i32;

    let payload = serde_json::json!({
        "event": "message_created",
        "id": id,
        "sender_id": body.sender_id,
        "receiver_id": body.receiver_id,
        "group_id": body.group_id,
        "message_type": body.message_type,
        "content": body.content
    });

    let msg = payload.to_string();

    let users = data.users.lock().unwrap();





    // ================= PUBLIC =================
    if body.message_type == "public" {
        for (_, tx) in users.iter() {
            let _ = tx.send(msg.clone());
        }
    }





    // ================= PRIVATE =================
    else if body.message_type == "private" {
        if let Some(rid) = body.receiver_id {

            if let Some(tx) = users.get(&rid) {
                let _ = tx.send(msg.clone());
            }

            if let Some(tx) = users.get(&body.sender_id) {
                let _ = tx.send(msg.clone());
            }
        }
    }





    // ================= GROUP =================
    else if body.message_type == "group" {
        if let Some(gid) = body.group_id {

            if let Some(members) =
                data.groups.lock().unwrap().get(&gid)
            {
                for uid in members {
                    if let Some(tx) = users.get(uid) {
                        let _ = tx.send(msg.clone());
                    }
                }
            }
        }
    }

    HttpResponse::Ok().json(payload)
}





// ================= GROUP ADD =================

async fn add_group_member(
    data: web::Data<AppState>,
    path: web::Path<(i32, i32)>,
) -> HttpResponse {

    let (group_id, user_id) = path.into_inner();

    data.groups
        .lock()
        .unwrap()
        .entry(group_id)
        .or_insert(HashSet::new())
        .insert(user_id);

    HttpResponse::Ok().json("added")
}





// ================= MAIN =================

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let pool = db::connect_db().await;

    let state = web::Data::new(AppState {
        users: Mutex::new(HashMap::new()),
        groups: Mutex::new(HashMap::new()),
    });

    println!("SERVER RUNNING => 8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(state.clone())

            .route("/ws/{id}", web::get().to(ws_handler))
            .route("/messages", web::post().to(create_message))
            .route("/groups/{group_id}/{user_id}", web::post().to(add_group_member))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

















// // src/main.rs

// use std::{
//     collections::HashMap,
//     sync::Arc,
// };

// use actix_web::{
//     web,
//     App,
//     HttpRequest,
//     HttpResponse,
//     HttpServer,
//     Responder,
// };

// use actix_ws::Message;

// use futures_util::StreamExt;

// use tokio::sync::{
//     RwLock,
//     mpsc::{
//         self,
//         UnboundedSender,
//     },
// };

// use serde::{
//     Serialize,
//     Deserialize,
// };

// use sqlx::FromRow;

// mod db;

// use db::connect_db;

// type Tx =
//     UnboundedSender<String>;

// struct ConnectionManager {

//     users:
//         RwLock<HashMap<i32, Tx>>,
// }

// impl ConnectionManager {

//     fn new() -> Self {

//         Self {
//             users:
//                 RwLock::new(HashMap::new())
//         }
//     }

//     async fn connect(
//         &self,
//         user_id: i32,
//         tx: Tx,
//     ) {

//         self.users
//             .write()
//             .await
//             .insert(user_id, tx);
//     }

//     async fn disconnect(
//         &self,
//         user_id: i32,
//     ) {

//         self.users
//             .write()
//             .await
//             .remove(&user_id);
//     }

//     async fn broadcast(
//         &self,
//         message: String,
//     ) {

//         let users =
//             self.users.read().await;

//         for (_, tx) in users.iter() {

//             let _ =
//                 tx.send(message.clone());
//         }
//     }
// }

// #[derive(Debug, Serialize, Deserialize)]

// struct CreateMessage {

//     sender_id: i32,

//     receiver_id: i32,

//     content: String,
// }

// #[derive(Debug, Serialize, Deserialize)]

// struct UpdateMessage {

//     content: String,
// }

// #[derive(Debug, Serialize, Deserialize)]

// struct WsResponse {

//     event: String,

//     data: serde_json::Value,
// }

// #[derive(Debug, Serialize, FromRow)]

// struct MessageModel {

//     id: i64,

//     sender_id: i32,

//     receiver_id: i32,

//     content: String,
// }

// struct AppState {

//     pool: sqlx::MySqlPool,

//     manager:
//         Arc<ConnectionManager>,
// }

// async fn ws_handler(
//     req: HttpRequest,
//     stream: web::Payload,
//     state: web::Data<AppState>,
// ) -> HttpResponse {

//     let (
//         response,
//         mut session,
//         mut msg_stream,
//     ) = actix_ws::handle(&req, stream)
//         .unwrap();

//     let query =
//         web::Query::<HashMap<String, String>>
//             ::from_query(req.query_string())
//             .unwrap();

//     let user_id =
//         query
//             .get("user_id")
//             .unwrap()
//             .parse::<i32>()
//             .unwrap();

//     let (
//         tx,
//         mut rx,
//     ) = mpsc::unbounded_channel::<String>();

//     state
//         .manager
//         .connect(user_id, tx)
//         .await;

//     println!(
//         "USER {} CONNECTED",
//         user_id
//     );

//     let manager =
//         state.manager.clone();

//     actix_web::rt::spawn(async move {

//         loop {

//             tokio::select! {

//                 Some(msg) = msg_stream.next() => {

//                     match msg {

//                         Ok(Message::Ping(bytes)) => {

//                             let _ =
//                                 session
//                                     .pong(&bytes)
//                                     .await;
//                         }

//                         Ok(Message::Close(_)) => {

//                             manager
//                                 .disconnect(user_id)
//                                 .await;

//                             println!(
//                                 "USER {} DISCONNECTED",
//                                 user_id
//                             );

//                             break;
//                         }

//                         _ => {}
//                     }
//                 }

//                 Some(server_msg) = rx.recv() => {

//                     let _ =
//                         session
//                             .text(server_msg)
//                             .await;
//                 }
//             }
//         }
//     });

//     response
// }

// async fn create_message(
//     body: web::Json<CreateMessage>,
//     state: web::Data<AppState>,
// ) -> impl Responder {

//     let result =
//         sqlx::query(
//             r#"
//             INSERT INTO messages
//             (
//                 sender_id,
//                 receiver_id,
//                 content
//             )
//             VALUES (?, ?, ?)
//             "#
//         )

//         .bind(body.sender_id)

//         .bind(body.receiver_id)

//         .bind(&body.content)

//         .execute(&state.pool)

//         .await;

//     match result {

//         Ok(res) => {

//             let id =
//                 res.last_insert_id();

//             let data =
//                 serde_json::json!({
//                     "id": id,
//                     "sender_id": body.sender_id,
//                     "receiver_id": body.receiver_id,
//                     "content": body.content
//                 });

//             let ws =
//                 WsResponse {

//                     event:
//                         String::from(
//                             "create_message"
//                         ),

//                     data,
//                 };

//             let text =
//                 serde_json::to_string(&ws)
//                     .unwrap();

//             state
//                 .manager
//                 .broadcast(text)
//                 .await;

//             HttpResponse::Ok()
//                 .json(ws)
//         }

//         Err(err) => {

//             println!(
//                 "CREATE ERROR => {:?}",
//                 err
//             );

//             HttpResponse::InternalServerError()
//                 .body("DATABASE ERROR")
//         }
//     }
// }

// async fn get_messages(
//     state: web::Data<AppState>,
// ) -> impl Responder {

//     let result =
//         sqlx::query_as::<_, MessageModel>(
//             r#"
//             SELECT
//                 id,
//                 sender_id,
//                 receiver_id,
//                 content
//             FROM messages
//             ORDER BY id DESC
//             "#
//         )

//         .fetch_all(&state.pool)

//         .await;

//     match result {

//         Ok(data) => {
//             HttpResponse::Ok().json(data)
//         }

//         Err(err) => {

//             println!(
//                 "GET ERROR => {:?}",
//                 err
//             );

//             HttpResponse::InternalServerError()
//                 .body("DATABASE ERROR")
//         }
//     }
// }

// async fn update_message(
//     path: web::Path<i64>,
//     body: web::Json<UpdateMessage>,
//     state: web::Data<AppState>,
// ) -> impl Responder {

//     let id =
//         path.into_inner();

//     let result =
//         sqlx::query(
//             r#"
//             UPDATE messages
//             SET content = ?
//             WHERE id = ?
//             "#
//         )

//         .bind(&body.content)

//         .bind(id)

//         .execute(&state.pool)

//         .await;

//     match result {

//         Ok(res) => {

//             if res.rows_affected() == 0 {

//                 return HttpResponse::NotFound()
//                     .body("MESSAGE NOT FOUND");
//             }

//             let data =
//                 serde_json::json!({
//                     "id": id,
//                     "content": body.content
//                 });

//             let ws =
//                 WsResponse {

//                     event:
//                         String::from(
//                             "update_message"
//                         ),

//                     data,
//                 };

//             let text =
//                 serde_json::to_string(&ws)
//                     .unwrap();

//             state
//                 .manager
//                 .broadcast(text)
//                 .await;

//             HttpResponse::Ok()
//                 .json(ws)
//         }

//         Err(err) => {

//             println!(
//                 "UPDATE ERROR => {:?}",
//                 err
//             );

//             HttpResponse::InternalServerError()
//                 .body("DATABASE ERROR")
//         }
//     }
// }

// async fn delete_message(
//     path: web::Path<i64>,
//     state: web::Data<AppState>,
// ) -> impl Responder {

//     let id =
//         path.into_inner();

//     let result =
//         sqlx::query(
//             r#"
//             DELETE FROM messages
//             WHERE id = ?
//             "#
//         )

//         .bind(id)

//         .execute(&state.pool)

//         .await;

//     match result {

//         Ok(res) => {

//             if res.rows_affected() == 0 {

//                 return HttpResponse::NotFound()
//                     .body("MESSAGE NOT FOUND");
//             }

//             let data =
//                 serde_json::json!({
//                     "id": id
//                 });

//             let ws =
//                 WsResponse {

//                     event:
//                         String::from(
//                             "delete_message"
//                         ),

//                     data,
//                 };

//             let text =
//                 serde_json::to_string(&ws)
//                     .unwrap();

//             state
//                 .manager
//                 .broadcast(text)
//                 .await;

//             HttpResponse::Ok()
//                 .json(ws)
//         }

//         Err(err) => {

//             println!(
//                 "DELETE ERROR => {:?}",
//                 err
//             );

//             HttpResponse::InternalServerError()
//                 .body("DATABASE ERROR")
//         }
//     }
// }

// #[actix_web::main]

// async fn main() -> std::io::Result<()> {

//     let pool =
//         connect_db().await;

//     let manager =
//         Arc::new(
//             ConnectionManager::new()
//         );

//     println!("SERVER RUNNING => 8080");

//     HttpServer::new(move || {

//         App::new()

//             .app_data(
//                 web::Data::new(
//                     AppState {
//                         pool: pool.clone(),
//                         manager: manager.clone(),
//                     }
//                 )
//             )

//             .route(
//                 "/ws",
//                 web::get().to(ws_handler)
//             )

//             .route(
//                 "/messages",
//                 web::post().to(create_message)
//             )

//             .route(
//                 "/messages",
//                 web::get().to(get_messages)
//             )

//             .route(
//                 "/messages/{id}",
//                 web::put().to(update_message)
//             )

//             .route(
//                 "/messages/{id}",
//                 web::delete().to(delete_message)
//             )
//     })

//     .bind(("127.0.0.1", 8080))?

//     .run()

//     .await
// }









// use actix_web::{
//     web,
//     App,
//     HttpRequest,
//     HttpResponse,
//     HttpServer,
// };

// use actix_ws::Message;

// use futures_util::StreamExt;

// async fn ws_handler(
//     req: HttpRequest,
//     stream: web::Payload,
// ) -> HttpResponse {

//     let (
//         response,
//         mut session,
//         mut msg_stream
//     ) = actix_ws::handle(&req, stream)
//         .unwrap();

//     println!("CLIENT CONNECTED");

//     actix_web::rt::spawn(async move {

//         while let Some(msg) =
//             msg_stream.next().await
//         {

//             match msg {

//                 Ok(Message::Text(text)) => {

//                     println!(
//                         "CLIENT SEND => {}",
//                         text
//                     );

//                     let reply =
//                         format!(
//                             "SERVER REPLY => {}",
//                             text
//                         );

//                     let _ =
//                         session
//                             .text(reply)
//                             .await;
//                 }

//                 Ok(Message::Ping(bytes)) => {

//                     println!("PING");

//                     let _ =
//                         session
//                             .pong(&bytes)
//                             .await;
//                 }

//                 Ok(Message::Close(_)) => {

//                     println!(
//                         "CLIENT DISCONNECTED"
//                     );

//                     break;
//                 }

//                 _ => {}
//             }
//         }
//     });

//     response
// }

// #[actix_web::main]

// async fn main() -> std::io::Result<()> {

//     println!("SERVER RUNNING => 8080");

//     HttpServer::new(|| {

//         App::new()

//             .route(
//                 "/ws",
//                 web::get().to(ws_handler)
//             )
//     })

//     .bind(("127.0.0.1", 8080))?

//     .run()

//     .await
// }