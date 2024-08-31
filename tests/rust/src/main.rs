#[tokio::main]
async fn main() {
    test_prepared_statements().await;
}

async fn test_prepared_statements() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db")
        .await
        .unwrap();

    let mut handles = Vec::new();

    for _ in 0..5 {
        let pool = pool.clone();
        let handle = tokio::task::spawn(async move {
            for i in 0..1000 {
                match sqlx::query(&format!("SELECT {:?}", i % 5)).fetch_all(&pool).await {
                    Ok(_) => (),
                    Err(err) => {
                        panic!("prepared statement error: {}", err);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
