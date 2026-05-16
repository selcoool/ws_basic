// DATABASE_URL=mysql://root:123456@localhost/chat_app




// ws://127.0.0.1:8080/ws/1
// ws://127.0.0.1:8080/ws/2

// {
//   "sender_id": 1,
//   "message_type": "public",
//   "content": "hello everyone"
// }


// ????????????????????????????????


// http://127.0.0.1:8080/messages

// {
//   "sender_id": 1,
//   "receiver_id": 2,
//   "message_type": "private",
//   "content": "hello user 2"
// }












// ??????????????????????


// Step 1: add user vào group

// 👉 Postman → HTTP

// POST http://127.0.0.1:8080/groups/1/1
// POST http://127.0.0.1:8080/groups/1/2

// 👉 Nghĩa là:

// group 1 có user 1
// group 1 có user 2



// POST http://127.0.0.1:8080/messages
// {
//   "sender_id": 1,
//   "group_id": 1,
//   "message_type": "group",
//   "content": "hello group"
// }


use sqlx::{
    mysql::MySqlPoolOptions,
    MySqlPool,
};

pub async fn connect_db() -> MySqlPool {

    dotenv::dotenv().ok();

    let database_url =
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL NOT FOUND");

    MySqlPoolOptions::new()

        .max_connections(10)

        .connect(&database_url)

        .await

        .expect("CANNOT CONNECT DATABASE")
}