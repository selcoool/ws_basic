mod db;

use actix_web::{
    web,
    App,
    HttpRequest,
    HttpResponse,
    HttpServer,
    Responder,
};

use actix_ws::Message;

use futures_util::StreamExt;

use serde::Deserialize;

use sqlx::{
    MySqlPool,
    Row
};

use std::{
    collections::HashMap,
    sync::Arc
};

use tokio::sync::{
    Mutex,
    mpsc::UnboundedSender
};

use db::connect_db;

#[derive(Clone)]

struct AppState {

    clients: Arc<
        Mutex<
            HashMap<
                i64,
                UnboundedSender<String>
            >
        >
    >
}

#[derive(Deserialize)]

struct CreateUserRequest {

    username: String,

    display_name: String,

    avatar: Option<String>
}

#[derive(Deserialize)]

struct CreateGroupRequest {

    title: String
}

#[derive(Deserialize)]

struct AddMemberRequest {

    conversation_id: i64,

    user_id: i64
}

#[derive(Deserialize)]

struct PrivateMessageRequest {

    sender_id: i64,

    receiver_id: i64,

    content: String
}

#[derive(Deserialize)]

struct GroupMessageRequest {

    sender_id: i64,

    conversation_id: i64,

    content: String
}

#[derive(Deserialize)]

struct PublicMessageRequest {

    sender_id: i64,

    content: String
}

async fn ws_handler(

    req: HttpRequest,

    stream: web::Payload,

    path: web::Path<i64>,

    state: web::Data<AppState>

) -> HttpResponse {

    let user_id =
        path.into_inner();

    let (
        response,
        mut session,
        mut msg_stream
    ) =
        actix_ws::handle(
            &req,
            stream
        )
        .unwrap();

    println!(
        "USER CONNECTED => {}",
        user_id
    );

    let (
        tx,
        mut rx
    ) =
        tokio::sync::mpsc
            ::unbounded_channel::<String>();

    state
        .clients
        .lock()
        .await
        .insert(user_id, tx);

    let state_clone =
        state.clone();

    actix_web::rt::spawn(async move {

        loop {

            tokio::select! {

                Some(server_msg) =
                    rx.recv() => {

                    let _ =
                        session
                            .text(server_msg)
                            .await;
                }

                Some(msg) =
                    msg_stream.next() => {

                    match msg {

                        Ok(Message::Close(_)) => {

                            println!(
                                "USER DISCONNECTED => {}",
                                user_id
                            );

                            state_clone
                                .clients
                                .lock()
                                .await
                                .remove(&user_id);

                            break;
                        }

                        _ => {}
                    }
                }

                else => break
            }
        }
    });

    response
}

async fn create_user(

    pool: web::Data<MySqlPool>,

    body: web::Json<CreateUserRequest>

) -> impl Responder {

    let result =
        sqlx::query(
            "
            INSERT INTO users
            (
                username,
                display_name,
                avatar
            )
            VALUES (?, ?, ?)
            "
        )

        .bind(&body.username)

        .bind(&body.display_name)

        .bind(&body.avatar)

        .execute(pool.get_ref())

        .await

        .unwrap();

    HttpResponse::Ok().json(
        serde_json::json!({

            "success": true,

            "user_id":
                result.last_insert_id()
        })
    )
}

async fn create_group(

    pool: web::Data<MySqlPool>,

    body: web::Json<CreateGroupRequest>

) -> impl Responder {

    let result =
        sqlx::query(
            "
            INSERT INTO conversations
            (
                type,
                title
            )
            VALUES
            (
                'group',
                ?
            )
            "
        )

        .bind(&body.title)

        .execute(pool.get_ref())

        .await

        .unwrap();

    HttpResponse::Ok().json(
        serde_json::json!({

            "success": true,

            "conversation_id":
                result.last_insert_id()
        })
    )
}

async fn add_member(

    pool: web::Data<MySqlPool>,

    body: web::Json<AddMemberRequest>

) -> impl Responder {

    sqlx::query(
        "
        INSERT INTO conversation_members
        (
            conversation_id,
            user_id
        )
        VALUES (?, ?)
        "
    )

    .bind(body.conversation_id)

    .bind(body.user_id)

    .execute(pool.get_ref())

    .await

    .unwrap();

    HttpResponse::Ok().json(
        serde_json::json!({

            "success": true
        })
    )
}

async fn private_message(

    pool: web::Data<MySqlPool>,

    state: web::Data<AppState>,

    body: web::Json<PrivateMessageRequest>

) -> impl Responder {

    let existing =
        sqlx::query(
            "
            SELECT c.id
            FROM conversations c

            JOIN conversation_members m1
            ON c.id = m1.conversation_id

            JOIN conversation_members m2
            ON c.id = m2.conversation_id

            WHERE c.type = 'private'

            AND m1.user_id = ?

            AND m2.user_id = ?

            LIMIT 1
            "
        )

        .bind(body.sender_id)

        .bind(body.receiver_id)

        .fetch_optional(pool.get_ref())

        .await

        .unwrap();

    let conversation_id: i64;

    if let Some(row) = existing {

        conversation_id =
            row.get("id");

    } else {

        let create =
            sqlx::query(
                "
                INSERT INTO conversations(type)
                VALUES('private')
                "
            )

            .execute(pool.get_ref())

            .await

            .unwrap();

        conversation_id =
            create.last_insert_id()
                as i64;

        sqlx::query(
            "
            INSERT INTO conversation_members
            (
                conversation_id,
                user_id
            )
            VALUES (?, ?)
            "
        )

        .bind(conversation_id)

        .bind(body.sender_id)

        .execute(pool.get_ref())

        .await

        .unwrap();

        sqlx::query(
            "
            INSERT INTO conversation_members
            (
                conversation_id,
                user_id
            )
            VALUES (?, ?)
            "
        )

        .bind(conversation_id)

        .bind(body.receiver_id)

        .execute(pool.get_ref())

        .await

        .unwrap();
    }

    let user =
        sqlx::query(
            "
            SELECT *
            FROM users
            WHERE id = ?
            "
        )

        .bind(body.sender_id)

        .fetch_one(pool.get_ref())

        .await

        .unwrap();

    let insert =
        sqlx::query(
            "
            INSERT INTO messages
            (
                conversation_id,
                sender_id,
                message_type,
                content
            )
            VALUES (?, ?, 'private', ?)
            "
        )

        .bind(conversation_id)

        .bind(body.sender_id)

        .bind(&body.content)

        .execute(pool.get_ref())

        .await

        .unwrap();

    let payload =
        serde_json::json!({

            "event":
                "private_message",

            "message": {

                "id":
                    insert.last_insert_id(),

                "conversation_id":
                    conversation_id,

                "sender": {

                    "id":
                        user.get::<i64, _>("id"),

                    "display_name":
                        user.get::<String, _>(
                            "display_name"
                        )
                },

                "receiver_id":
                    body.receiver_id,

                "content":
                    body.content
            }
        });

    let text =
        payload.to_string();

    let clients =
        state.clients
            .lock()
            .await;

    // ================= RECEIVER =================

    if let Some(tx) =
        clients.get(&body.receiver_id)
    {
        let _ =
            tx.send(
                text.clone()
            );
    }

    // ================= SENDER =================

    if let Some(tx) =
        clients.get(&body.sender_id)
    {
        let _ =
            tx.send(
                text.clone()
            );
    }

    HttpResponse::Ok().json(payload)
}

async fn group_message(

    pool: web::Data<MySqlPool>,

    state: web::Data<AppState>,

    body: web::Json<GroupMessageRequest>

) -> impl Responder {

    let user =
        sqlx::query(
            "
            SELECT *
            FROM users
            WHERE id = ?
            "
        )

        .bind(body.sender_id)

        .fetch_one(pool.get_ref())

        .await

        .unwrap();

    let insert =
        sqlx::query(
            "
            INSERT INTO messages
            (
                conversation_id,
                sender_id,
                message_type,
                content
            )
            VALUES (?, ?, 'group', ?)
            "
        )

        .bind(body.conversation_id)

        .bind(body.sender_id)

        .bind(&body.content)

        .execute(pool.get_ref())

        .await

        .unwrap();

    let payload =
        serde_json::json!({

            "event":
                "group_message",

            "message": {

                "id":
                    insert.last_insert_id(),

                "conversation_id":
                    body.conversation_id,

                "sender": {

                    "id":
                        user.get::<i64, _>("id"),

                    "display_name":
                        user.get::<String, _>(
                            "display_name"
                        )
                },

                "content":
                    body.content
            }
        });

    let members =
        sqlx::query(
            "
            SELECT user_id
            FROM conversation_members
            WHERE conversation_id = ?
            "
        )

        .bind(body.conversation_id)

        .fetch_all(pool.get_ref())

        .await

        .unwrap();

    let clients =
        state.clients
            .lock()
            .await;

    for row in members {

        let uid: i64 =
            row.get("user_id");

        if let Some(tx) =
            clients.get(&uid)
        {
            let _ =
                tx.send(
                    payload.to_string()
                );
        }
    }

    HttpResponse::Ok().json(payload)
}

async fn public_message(

    pool: web::Data<MySqlPool>,

    state: web::Data<AppState>,

    body: web::Json<PublicMessageRequest>

) -> impl Responder {

    let user =
        sqlx::query(
            "
            SELECT *
            FROM users
            WHERE id = ?
            "
        )

        .bind(body.sender_id)

        .fetch_one(pool.get_ref())

        .await

        .unwrap();

    let insert =
        sqlx::query(
            "
            INSERT INTO messages
            (
                sender_id,
                message_type,
                content
            )
            VALUES (?, 'public', ?)
            "
        )

        .bind(body.sender_id)

        .bind(&body.content)

        .execute(pool.get_ref())

        .await

        .unwrap();

    let payload =
        serde_json::json!({

            "event":
                "public_message",

            "message": {

                "id":
                    insert.last_insert_id(),

                "sender": {

                    "id":
                        user.get::<i64, _>("id"),

                    "display_name":
                        user.get::<String, _>(
                            "display_name"
                        )
                },

                "content":
                    body.content
            }
        });

    let clients =
        state.clients
            .lock()
            .await;

    for (_, tx) in clients.iter() {

        let _ =
            tx.send(
                payload.to_string()
            );
    }

    HttpResponse::Ok().json(payload)
}

#[actix_web::main]

async fn main() -> std::io::Result<()> {

    let pool =
        connect_db().await;

    let state =
        AppState {

            clients:
                Arc::new(
                    Mutex::new(
                        HashMap::new()
                    )
                )
        };

    println!(
        "SERVER RUNNING => 8080"
    );

    HttpServer::new(move || {

        App::new()

            .app_data(
                web::Data::new(
                    pool.clone()
                )
            )

            .app_data(
                web::Data::new(
                    state.clone()
                )
            )

            .route(
                "/ws/{user_id}",
                web::get().to(
                    ws_handler
                )
            )

            .route(
                "/users",
                web::post().to(
                    create_user
                )
            )

            .route(
                "/groups",
                web::post().to(
                    create_group
                )
            )

            .route(
                "/members",
                web::post().to(
                    add_member
                )
            )

            .route(
                "/private-message",
                web::post().to(
                    private_message
                )
            )

            .route(
                "/group-message",
                web::post().to(
                    group_message
                )
            )

            .route(
                "/public-message",
                web::post().to(
                    public_message
                )
            )
    })

    .bind(("127.0.0.1", 8080))?

    .run()

    .await
}





// mod db;

// use actix_web::{
//     rt,
//     web,
//     App,
//     Error,
//     HttpRequest,
//     HttpResponse,
//     HttpServer,
// };

// use actix_ws;

// use futures_util::StreamExt;

// use serde::Deserialize;

// use sqlx::MySqlPool;

// use std::{
//     collections::{HashMap, HashSet},
//     sync::Mutex,
// };

// use tokio::sync::mpsc;





// // ================= STATE =================

// struct AppState {
//     users: Mutex<HashMap<i32, mpsc::UnboundedSender<String>>>,
//     groups: Mutex<HashMap<i32, HashSet<i32>>>,
// }





// // ================= REQUEST =================

// #[derive(Deserialize)]
// struct CreateMessage {
//     sender_id: i32,
//     sender_name: String,
//     receiver_id: Option<i32>,
//     group_id: Option<i32>,
//     message_type: String,
//     content: String,
// }





// // ================= WS HANDLER =================

// async fn ws_handler(
//     req: HttpRequest,
//     stream: web::Payload,
//     path: web::Path<i32>,
//     data: web::Data<AppState>,
// ) -> Result<HttpResponse, Error> {

//     let user_id = path.into_inner();

//     let (res, mut session, mut msg_stream) =
//         actix_ws::handle(&req, stream)?;

//     let (tx, mut rx) = mpsc::unbounded_channel::<String>();

//     data.users.lock().unwrap().insert(user_id, tx);

//     let state = data.clone();





//     rt::spawn(async move {
//         loop {
//             tokio::select! {

//                 Some(msg) = rx.recv() => {
//                     let _ = session.text(msg).await;
//                 }

//                 Some(Ok(msg)) = msg_stream.next() => {
//                     if let actix_ws::Message::Close(_) = msg {
//                         break;
//                     }
//                 }

//                 else => break
//             }
//         }

//         state.users.lock().unwrap().remove(&user_id);
//     });

//     Ok(res)
// }





// // ================= CREATE MESSAGE =================

// async fn create_message(
//     data: web::Data<AppState>,
//     pool: web::Data<MySqlPool>,
//     body: web::Json<CreateMessage>,
// ) -> HttpResponse {

//     let result = sqlx::query(
//         r#"
//         INSERT INTO messages
//         (sender_id, receiver_id, group_id, message_type, content)
//         VALUES (?, ?, ?, ?, ?)
//         "#
//     )
//     .bind(body.sender_id)
//     .bind(body.receiver_id)
//     .bind(body.group_id)
//     .bind(&body.message_type)
//     .bind(&body.content)
//     .execute(pool.get_ref())
//     .await;

//     if result.is_err() {
//         return HttpResponse::InternalServerError().body("DB error");
//     }

//     let id = result.unwrap().last_insert_id() as i32;





//     let payload = serde_json::json!({
//         "id": id,
//         "sender_id": body.sender_id,
//         "sender_name": body.sender_name,
//         "receiver_id": body.receiver_id,
//         "group_id": body.group_id,
//         "message_type": body.message_type,
//         "content": body.content
//     });

//     let msg = payload.to_string();





//     let users = data.users.lock().unwrap();





//     // ================= PUBLIC =================
//     if body.message_type == "public" {
//         for (_, tx) in users.iter() {
//             let _ = tx.send(msg.clone());
//         }
//     }





//     // ================= PRIVATE =================
//     else if body.message_type == "private" {
//         if let Some(rid) = body.receiver_id {
//             if let Some(tx) = users.get(&rid) {
//                 let _ = tx.send(msg.clone());
//             }
//             if let Some(tx) = users.get(&body.sender_id) {
//                 let _ = tx.send(msg.clone());
//             }
//         }
//     }





//     // ================= GROUP (FIXED 100%) =================
//     else if body.message_type == "group" {

//         if let Some(gid) = body.group_id {

//             let groups = data.groups.lock().unwrap();

//             // group không tồn tại
//             if !groups.contains_key(&gid) {
//                 return HttpResponse::BadRequest().body("Group not found");
//             }

//             let members = groups.get(&gid).unwrap();

//             // sender phải là member
//             if !members.contains(&body.sender_id) {
//                 return HttpResponse::Forbidden().body("Not group member");
//             }

//             // broadcast only members
//             for uid in members {
//                 if let Some(tx) = users.get(uid) {
//                     let _ = tx.send(msg.clone());
//                 }
//             }
//         }
//     }

//     HttpResponse::Ok().json(payload)
// }





// // ================= ADD GROUP MEMBER =================

// async fn add_group_member(
//     data: web::Data<AppState>,
//     path: web::Path<(i32, i32)>,
// ) -> HttpResponse {

//     let (group_id, user_id) = path.into_inner();

//     let mut groups = data.groups.lock().unwrap();

//     groups
//         .entry(group_id)
//         .or_insert_with(HashSet::new)
//         .insert(user_id);

//     HttpResponse::Ok().json("added")
// }





// // ================= MAIN =================

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {

//     let pool = db::connect_db().await;

//     let state = web::Data::new(AppState {
//         users: Mutex::new(HashMap::new()),
//         groups: Mutex::new(HashMap::new()), // ✅ FIXED
//     });

//     println!("SERVER RUNNING => 8080");

//     HttpServer::new(move || {
//         App::new()
//             .app_data(web::Data::new(pool.clone()))
//             .app_data(state.clone())

//             .route("/ws/{id}", web::get().to(ws_handler))
//             .route("/messages", web::post().to(create_message))
//             .route("/groups/{group_id}/{user_id}", web::post().to(add_group_member))
//     })
//     .bind(("127.0.0.1", 8080))?
//     .run()
//     .await
// }














// mod db;

// use actix_web::{
//     rt,
//     web,
//     App,
//     Error,
//     HttpRequest,
//     HttpResponse,
//     HttpServer,
// };

// use actix_ws;

// use futures_util::StreamExt;

// use serde::Deserialize;

// use sqlx::MySqlPool;

// use std::{
//     collections::{HashMap, HashSet},
//     sync::Mutex,
// };

// use tokio::sync::mpsc;





// // ================= STATE =================

// struct AppState {
//     users: Mutex<HashMap<i32, mpsc::UnboundedSender<String>>>,
//     groups: Mutex<HashMap<i32, HashSet<i32>>>,
// }





// // ================= REQUEST =================

// #[derive(Deserialize)]
// struct CreateMessage {
//     sender_id: i32,
//     receiver_id: Option<i32>,
//     group_id: Option<i32>,
//     message_type: String,
//     content: String,
// }





// // ================= WS =================

// async fn ws_handler(
//     req: HttpRequest,
//     stream: web::Payload,
//     path: web::Path<i32>,
//     data: web::Data<AppState>,
// ) -> Result<HttpResponse, Error> {

//     let user_id = path.into_inner();

//     let (res, mut session, mut msg_stream) =
//         actix_ws::handle(&req, stream)?;

//     let (tx, mut rx) = mpsc::unbounded_channel::<String>();

//     data.users.lock().unwrap().insert(user_id, tx);

//     let state = data.clone();





//     rt::spawn(async move {

//         loop {
//             tokio::select! {

//                 Some(msg) = rx.recv() => {
//                     let _ = session.text(msg).await;
//                 }

//                 Some(Ok(msg)) = msg_stream.next() => {
//                     if let actix_ws::Message::Close(_) = msg {
//                         break;
//                     }
//                 }

//                 else => break
//             }
//         }

//         state.users.lock().unwrap().remove(&user_id);
//     });

//     Ok(res)
// }





// // ================= CREATE MESSAGE =================

// async fn create_message(
//     data: web::Data<AppState>,
//     pool: web::Data<MySqlPool>,
//     body: web::Json<CreateMessage>,
// ) -> HttpResponse {

//     let result = sqlx::query(
//         r#"
//         INSERT INTO messages
//         (sender_id, receiver_id, group_id, message_type, content)
//         VALUES (?, ?, ?, ?, ?)
//         "#
//     )
//     .bind(body.sender_id)
//     .bind(body.receiver_id)
//     .bind(body.group_id)
//     .bind(&body.message_type)
//     .bind(&body.content)
//     .execute(pool.get_ref())
//     .await
//     .unwrap();

//     let id = result.last_insert_id() as i32;

//     let payload = serde_json::json!({
//         "event": "message_created",
//         "id": id,
//         "sender_id": body.sender_id,
//         "receiver_id": body.receiver_id,
//         "group_id": body.group_id,
//         "message_type": body.message_type,
//         "content": body.content
//     });

//     let msg = payload.to_string();

//     let users = data.users.lock().unwrap();





//     // ================= PUBLIC =================
//     if body.message_type == "public" {
//         for (_, tx) in users.iter() {
//             let _ = tx.send(msg.clone());
//         }
//     }





//     // ================= PRIVATE =================
//     else if body.message_type == "private" {
//         if let Some(rid) = body.receiver_id {

//             if let Some(tx) = users.get(&rid) {
//                 let _ = tx.send(msg.clone());
//             }

//             if let Some(tx) = users.get(&body.sender_id) {
//                 let _ = tx.send(msg.clone());
//             }
//         }
//     }





//     // ================= GROUP =================
//     else if body.message_type == "group" {
//         if let Some(gid) = body.group_id {

//             if let Some(members) =
//                 data.groups.lock().unwrap().get(&gid)
//             {
//                 for uid in members {
//                     if let Some(tx) = users.get(uid) {
//                         let _ = tx.send(msg.clone());
//                     }
//                 }
//             }
//         }
//     }

//     HttpResponse::Ok().json(payload)
// }





// // ================= GROUP ADD =================

// async fn add_group_member(
//     data: web::Data<AppState>,
//     path: web::Path<(i32, i32)>,
// ) -> HttpResponse {

//     let (group_id, user_id) = path.into_inner();

//     data.groups
//         .lock()
//         .unwrap()
//         .entry(group_id)
//         .or_insert(HashSet::new())
//         .insert(user_id);

//     HttpResponse::Ok().json("added")
// }





// // ================= MAIN =================

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {

//     let pool = db::connect_db().await;

//     let state = web::Data::new(AppState {
//         users: Mutex::new(HashMap::new()),
//         groups: Mutex::new(HashMap::new()),
//     });

//     println!("SERVER RUNNING => 8080");

//     HttpServer::new(move || {
//         App::new()
//             .app_data(web::Data::new(pool.clone()))
//             .app_data(state.clone())

//             .route("/ws/{id}", web::get().to(ws_handler))
//             .route("/messages", web::post().to(create_message))
//             .route("/groups/{group_id}/{user_id}", web::post().to(add_group_member))
//     })
//     .bind(("127.0.0.1", 8080))?
//     .run()
//     .await
// }

















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